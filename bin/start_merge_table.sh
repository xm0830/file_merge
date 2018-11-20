#!/bin/bash

if [[ $# -ne 6 ]];then
  echo -e "执行方式: $0 <参数1> <参数2> <参数3> <参数4> <参数5> <参数6> \n
  参数说明:
  \t参数1: 必填,要合并的表名(格式:库名.表名)
  \t参数2: 必填,要合并的表数据压缩格式:text(无压缩),gzip,lz4,bzip2,snappy
  \t参数3: 必填,临时输出库名
  \t参数4: 必填,起始时间dt(格式:yyyy-mm-dd或者yyyymmdd,取决于表中dt字段的格式)
  \t参数5: 必填,结束时间dt(格式:yyyy-mm-dd,取决于表中dt字段的格式,不包含该时间)
  \t参数6: 必填,通知人(用户邮箱,多个之间用逗号分隔),不通知填写空字符串 \n
  注意：脚本日志会重定向到标准输出中，HIVE任务相关的日志会重定向到标准错误中"
  exit 1
fi

full_table_name=$1
codec=$2
tmp_db_name=$3
start_dt=$4
end_dt=$5
mails=$6

dir=$(cd ../$(dirname $0);pwd)
queue_name=""
export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Xmx2048m"

function send_message()
{
  local msgURL='http://adp.data.autohome.com.cn/notice/msgApi/sendMsgApi.json'
  local mails=$1
  local title="$2"
  local mtitle=`echo "$title"`
  local msg="$3"
  local mmsg=`echo "$msg"|sed 's/"/\\\"/g;s/$/<br>/g;s/\t/ /g'|tr -d '\n'`

  users=`echo "$mails"|sed 's/^/{"mail":"/g;s/,/"},{"mail":"/g;s/$/"}/g'`

  data='{"type":"mail","title":"'$mtitle'","user":['$users'],"content":"'$mmsg'"}'

  echo "$data" > /tmp/jsonmail.html.$$

  curl -i -X POST -H "Accept:application/json" -H "Content-type:application/json;charset=UTF-8" --data-binary @/tmp/jsonmail.html.$$ $msgURL > /dev/null 2>&1
  rm -f /tmp/jsonmail.html.$$
}

function print_to_stdout()
{
  time=$(date "+%Y-%m-%d %H:%M:%S")
  if [[ $# != 2 ]];then
    echo -e "[INFO][${time}] $1" >&1
  else
    if [[ "$2" = "error" ]];then
      echo -e "[ERROR][${time}] $1" >&1
      if [[ "$mails" != "" ]];then
        echo -e "[INFO][${time}] 开始发送邮件通知用户: ${mails}" >&1
        send_message "$mails" "【小文件合并任务提醒】" "$1"
      fi
    else
      echo -e "[INFO][${time}] $1" >&1
    fi
  fi
}

function print_to_stderr()
{
  time=$(date "+%Y-%m-%d %H:%M:%S")

  if [[ "$2" = "error" ]];then
    echo -e "[ERROR][${time}] $1" >&2
  else
    echo -e "[INFO][${time}] $1" >&2
  fi
}

function validate_table_name()
{
  result=$(echo $1 | grep "\\.")
  if [[ "${result}" == "" ]];then
    print_to_stdout "表名: $1 的格式不符合: 库名.表名 的构成规则!" "error"
    exit 1
  fi
}

function validate_codec()
{
  if [[ "$1" != "text" ]] && [[ "$1" != "gzip" ]] && [[ "$1" != "lz4" ]] && [[ "$1" != "bzip2" ]] && [[ "$1" != "snappy" ]];then
    print_to_stdout "只支持: text(无压缩)、gzip、lz4、bzip2、snappy 这几种类型的压缩方式,用户输入压缩格式:$1" "error"
    exit 1
  fi
}

function validate_date()
{
  result=$(echo $1 | grep -E "([0-9]{4}-[0-9]{2}-[0-9]{2})|([0-9]{8})")
  if [[ "$result" == "" ]];then
    print_to_stdout "日期格式必须是: yyyy-mm-dd或者yyyymmdd,用户输入格式: $1" "error"
    exit 1
  fi
}

function validate_partition()
{
  result=$(echo $1 | grep -E "dt,?.*")
  if [[ "${result}" == "" ]];then
    print_to_stdout "分区格式: ${1} 必须包含:dt" "error"
    exit 1
  fi
}

function get_cols_before_parse()
{
  local full_table_name=$1
  all_cols=""
  for col in $(hive -e "desc $full_table_name" 2>/dev/null | awk '{print $1}')
  do
    all_cols="${all_cols},${col}"
  done

  if [[ $? -ne 0 ]];then
      print_to_stdout "调用hive获取表: $1 的列名和分区名失败！" "error"
      exit 1
  fi

  all_cols=${all_cols#*,}

  echo "$all_cols"
}

function get_replace_script()
{
  case $1 in
  text)
    replace_script="$dir/scripts/merge_replace_file_text.hql"
    ;;
  gzip)
    replace_script="$dir/scripts/merge_replace_file_gzip.hql"
    ;;
  lz4)
    replace_script="$dir/scripts/merge_replace_file_lz4.hql"
    ;;
  bzip2)
    replace_script="$dir/scripts/merge_replace_file_bzip2.hql"
    ;;
  snappy)
    replace_script="$dir/scripts/merge_replace_file_snappy.hql"
    ;;
  *)
    print_to_stdout "不支持的压缩格式: $codec" "error"
    exit 1
  esac

  echo "$replace_script"
}

# 校验输入的表名参数是否符合规则
validate_table_name $full_table_name

# 校验输入的压缩格式是否正确
validate_codec $codec

# 校验日期格式是否正确
validate_date $start_dt
validate_date $end_dt

# 获取表的列名和分区名
print_to_stdout "开始获取表: ${full_table_name} 的列名和分区名"
cols_before_parse=$(get_cols_before_parse ${full_table_name})

cols=$(echo $cols_before_parse | awk -F',#,#,' '{print $1}')
partition_cols=$(echo $cols_before_parse | awk -F',#,#,' '{print $2}')
print_to_stdout "获取到表:${full_table_name}的列名为:${cols}"
print_to_stdout "获取到表:${full_table_name}的分区为:${partition_cols}"

# 校验分区格式是否正确
validate_partition $partition_cols

# 根据压缩格式获取替换的脚本
replace_script=$(get_replace_script $codec)
print_to_stdout "将使用: $replace_script 进行替换"

current_dt=${start_dt}
while [[ 1 == 1 ]];
do
  if ([[ "$current_dt" == "$start_dt" ]] && [[ "$current_dt" < "$end_dt" ]]) || ([[ "$current_dt" > "$start_dt" ]] && [[ "$current_dt" < "$end_dt" ]]);then
    print_to_stdout "开始处理日期dt为：${current_dt} 的数据"
    print_to_stderr "开始处理日期dt为：${current_dt} 的数据"
   
    # 检查时间段是否在23:00-09:00之间
    current_hour=$(date +%T | awk -F':' '{print $1}')
    if [[ "$current_hour" > "23" ]] || [[ "$current_hour" < "09" ]];then
       print_to_stdout "为避免影响晚上重要任务的运行，程序将在23:00-09:00之间进入休眠状态"
       sleep 11h
    fi
 
    # 创建相同表结构的临时表
    table_name="merge_"${full_table_name##*.}
    hive -e "create table if not exists ${tmp_db_name}.${table_name} like ${full_table_name}" 2> /dev/null
    if [[ $? -ne 0 ]];then
      print_to_stdout "调用hive创建临时表: ${tmp_db_name}.${table_name} 失败！" "error"
      exit 1
    fi

    # 开始合并数据到临时表
    hive -hivevar queue_name=${queue_name} -hivevar tmp_db_name=${tmp_db_name} -hivevar table_name=${table_name} -hivevar partition_cols=${partition_cols} -hivevar cols=${cols} -hivevar full_table_name=${full_table_name} -hivevar dt=${current_dt}  -f $dir/scripts/merge_src_file.hql 1>&2
    if [[ $? -ne 0 ]];then
      print_to_stdout "调用hive合并表: ${full_table_name} 在${current_dt}的数据失败！" "error"
      exit 1
    fi
   
    # 开始校验合并后的数据
    validate_data=$(hive -hivevar queue_name=${queue_name} -hivevar tmp_db_name=${tmp_db_name} -hivevar table_name=${table_name} -hivevar cols=${cols} -hivevar full_table_name=${full_table_name} -hivevar dt=${current_dt} -f $dir/scripts/merge_validate_file.hql)
    if [[ $? -ne 0 ]];then
      print_to_stdout "调用hive校验表: ${full_table_name} 在${current_dt}的合并后的数据是否正确失败！" "error"
      exit 1
    fi
    if [[ ${validate_data} > 0 ]];then
      print_to_stdout "表: ${full_table_name} 在${current_dt}的合并后的数据与原始数据不一致，忽略替换操作，退出合并任务！" "error"
      exit 1
    fi

    # 开始替换原始表的数据
    hive -hivevar queue_name=${queue_name} -hivevar tmp_db_name=${tmp_db_name} -hivevar table_name=${table_name} -hivevar partition_cols=${partition_cols} -hivevar cols=${cols} -hivevar full_table_name=${full_table_name} -hivevar dt=${current_dt} -f ${replace_script} 1>&2
    if [[ $? -ne 0 ]];then
      print_to_stdout "调用hive替换表: ${full_table_name} 在${current_dt}的合并后的数据失败！" "error"
      exit 1
    fi

    # 开始校验替换后的数据
    validate_data=$(hive -hivevar queue_name=${queue_name} -hivevar tmp_db_name=${tmp_db_name} -hivevar table_name=${table_name} -hivevar cols=${cols} -hivevar full_table_name=${full_table_name} -hivevar dt=${current_dt} -f $dir/scripts/merge_validate_file.hql)
    if [[ $? -ne 0 ]];then
      print_to_stdout "调用hive校验表: ${full_table_name} 在${current_dt}的替换后的数据是否正确失败！" "error"
      exit 1
    fi
    if [[ ${validate_data} > 0 ]];then
      print_to_stdout "表: ${full_table_name} 在${current_dt}的合并后的数据与原始数据不一致，忽略替换操作，退出合并任务！" "error"
      exit 1
    fi

    print_to_stdout "处理日期dt为：${current_dt} 的数据成功!"
    print_to_stderr "处理日期dt为：${current_dt} 的数据成功!"

    if [[ "$current_dt" =~ "-" ]];then
      current_dt=$(date -d "$current_dt +1 day " +%Y-%m-%d)
    else
      current_dt=$(date -d "$current_dt +1 day " +%Y%m%d)
    fi
  else
    break
  fi
done

if [[ "$mails" != "" ]];then
  send_message "$mails" "【小文件合并任务提醒】" "您的合并任务已经完成,表: ${full_table_name},起始时间:${start_dt},结束时间:${end_dt}(不包含),请确认成功后务必手动删除${tmp_db_name}库中对应的临时表!"
fi