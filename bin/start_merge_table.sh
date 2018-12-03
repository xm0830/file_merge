#!/bin/bash

if [[ $# -ne 6 ]];then
  echo -e "执行方式: $0 <参数1> <参数2> <参数3> <参数4> <参数5> <参数6> \n
  参数说明:
  \t参数1: 必填,要合并的表名(格式:库名.表名)
  \t参数2: 必填,要合并的表数据压缩格式:text(无压缩),gzip,lz4,bzip2,snappy
  \t参数3: 必填,临时输出库名
  \t参数4: 必填,起始时间dt(格式:yyyy-mm-dd或者yyyymmdd,取决于表中dt字段的格式)
  \t参数5: 必填,结束时间dt(格式:yyyy-mm-dd,取决于表中dt字段的格式,不包含该时间)
  \t参数6: 必填,通知人(用户手机号,多个之间用逗号分隔),不通知填写空字符串 \n
  注意：脚本日志会重定向到标准输出中，HIVE任务相关的日志会重定向到标准错误中"
  exit 1
fi

full_table_name=$1
codec=$2
tmp_db_name=$3
start_dt=$4
end_dt=$5
users=$6

source ./common_util.sh

dir=$(cd ../$(dirname $0);pwd)
queue_name="root.q_ad.q_adlog_merge"
# queue_name="root.q_dtb.q_dw.q_dw_etl"
# queue_name="root.q_tongyong"
export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Xmx512m"

function validate_partition()
{
  result=$(echo $1  | grep -Ev ",dt,?.*" | grep -E "dt,?.*")
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

function get_validate_cols()
{
  local cols=$1
  local new_cols="md5(concat("
  array=(${cols//,/ })
  for col in ${array[@]}
  do
    if [[ "$new_cols" == "md5(concat(" ]];then
      new_cols="${new_cols}nvl($col, '')"
    else
      new_cols="${new_cols},nvl($col, '')"
    fi
  done
  new_cols="${new_cols}))"
  echo ${new_cols}
}

function build_validate_sql()
{
  local queue_name=$1
  local tmp_db_name=$2
  local table_name=$3
  local validate_cols=$4
  local full_table_name=$5
  local dt=$6
  echo -e "set hive.map.aggr=true;set hive.exec.parallel=true;set hive.exec.parallel.thread.number=2;set mapreduce.job.queuename=${queue_name};add jar viewfs://AutoLq2Cluster/user/xuming10797/bdp-udf-1.0-SNAPSHOT.jar;
create temporary function convert_as_bigint as 'com.autohome.bdp.udf.ConvertAsBigint';select
  count(1)
from
  (
    select
      hash(a.md5_value) % 50000 as hv,
      sum(convert_as_bigint(a.md5_value)) as md5_value
    from
      (
        select
          ${validate_cols} as md5_value
        from
          ${full_table_name}
        where
          dt = '${dt}'
      ) as a
    group by
      hash(a.md5_value) % 50000
  ) as c full
  outer join (
    select
      hash(b.md5_value) % 50000 as hv,
      sum(convert_as_bigint(b.md5_value)) as md5_value
    from
      (
        select
          ${validate_cols} as md5_value
        from
          ${tmp_db_name}.${table_name}
        where
          dt = '${dt}'
      ) as b
      group by
        hash(b.md5_value) % 50000
  ) as d on c.hv = d.hv
where
  c.md5_value != d.md5_value
  or c.md5_value is null
  or d.md5_value is null;"
}

# 校验输入的表名参数是否符合规则
validate_table_name $full_table_name

# 校验输入的压缩格式是否正确
validate_codec $codec

# 校验日期格式是否正确
if [[ -n "$start_dt" ]];then
  validate_date $start_dt
fi
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

# 获取表的日期级别的分区
dts=($(hive -e "show partitions ${full_table_name}" 2>/dev/null | awk -F'/' '{print $1}' | sort -u | awk -F'=' -v start_dt=$start_dt -v end_dt=$end_dt '{if ($2>=start_dt && $2<end_dt) print $2}'))
for ((i=1; i<=3; i ++))  
do  
  if [[ ${#dts[@]} -eq 0 ]];then
    sleep 5s
    dts=($(hive -e "show partitions ${full_table_name}" 2>/dev/null | awk -F'/' '{print $1}' | sort -u | awk -F'=' -v start_dt=$start_dt -v end_dt=$end_dt '{if ($2>=start_dt && $2<end_dt) print $2}'))
  else
    break
  fi
done 

for current_dt in ${dts[@]}
do
  print_to_stdout "开始处理日期dt为：${current_dt} 的数据"
  print_to_stderr "开始处理日期dt为：${current_dt} 的数据"
  
  if [[ "$start_dt" == "" ]];then
    start_dt="${current_dt}"
  fi

  # 检查时间段是否在23:00-09:00之间
  if [[ "$queue_name" != "root.q_ad.q_adlog_merge" ]];then
    current_hour=$(date +%T | awk -F':' '{print $1}')
    if [[ "$current_hour" > "22" ]] || [[ "$current_hour" < "09" ]];then
        print_to_stdout "为避免影响晚上重要任务的运行，程序将在23:00-09:00之间进入休眠状态"
        sleep 9h
    fi
  fi

  # 创建相同表结构的临时表
  table_name="merge_"${full_table_name##*.}
  hive -e "create table if not exists ${tmp_db_name}.${table_name} like ${full_table_name}" >&2
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
  validate_cols="$(get_validate_cols ${cols})"
  validate_sql=$(build_validate_sql "${queue_name}" "${tmp_db_name}" "${table_name}" "${validate_cols}" "${full_table_name}" "${current_dt}")
  # print_to_stdout "$validate_sql"
  validate_data=$(hive -e "${validate_sql}")
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
  validate_data=$(hive -e "${validate_sql}")
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
done

if [[ "$users" != "" ]];then
  send_message "$users" "【小文件合并任务提醒】您的合并任务已经完成,表: ${full_table_name},起始时间:${start_dt},结束时间:${end_dt}(不包含),请确认成功后务必手动删除${tmp_db_name}库中对应的临时表!"
fi