#!/bin/bash

if [[ $# -ne 4 ]];then
  echo -e "执行方式: $0 <参数1> <参数2> <参数3> <参数4> \n
  参数说明:
  \t参数1: 必填,要合并的表所在的文件名(每行一个表，格式:库名.表名)
  \t参数2: 必填,临时输出库名
  \t参数3: 必填,结束时间dt(格式:yyyy-mm-dd,取决于表中dt字段的格式,不包含该时间)
  \t参数4: 必填,通知人(用户手机号,多个之间用逗号分隔),不通知填写空字符串"
  exit 1
fi

source ./common_util.sh

table_list=$1
codec="gzip"
tmp_db_name=$2
start_dt=""
end_dt=$3
users=$4
merge_workers=10

log_dir=$(cd ../$(dirname $0);pwd)/logs

# 初始化日志目录
if [[ ! -d "${log_dir}" ]];then
  mkdir $log_dir
fi

# 校验列表文件是否存在
if [[ ! -f "${table_list}" ]];then
  print_to_stdout "列表文件: ${table_list} 不存在!" "error"
  exit 1
fi

# 校验其他传入参数
validate_codec $codec
validate_date $end_dt

# 批量提交任务
tables=($(cat ${table_list}))
for full_table_name in ${tables[@]}
do
  validate_table_name ${full_table_name}

  current_works=$(ps aux | grep "start_merge_table.sh" | grep -v "grep" | wc -l)
  while [[ 1 == 1 ]]
  do
      if [[ "$current_works" -gt "$merge_workers" ]];then
        sleep 10
        current_works=$(ps aux | grep "start_merge_table.sh" | grep -v "grep" | wc -l)
      else
        break
      fi
  done

  if [[ "$full_table_name" == "" ]];then
    continue
  fi

  table_name=${full_table_name##*.}
  time=$(date "+%Y-%m-%d %H:%M:%S")
  echo -e "[INFO][${time}] 开始提交合并表：${full_table_name} 的任务" >> ${log_dir}/start_batch_merge.log
  nohup sh ./start_merge_table.sh "$full_table_name" "$codec" "$tmp_db_name" "$start_dt" "$end_dt" "$users" >${log_dir}/mergelog_${table_name}.log 2>${log_dir}/hivelog_${table_name}.log &
done

# 等待所有任务运行完成
current_works=$(ps aux | grep "start_merge_table.sh" | grep -v "grep" | wc -l)
while [[ 1==1 ]]
do
    if [[ "$current_works" -ne 0 ]];then
        sleep 10
        current_works=$(ps aux | grep "start_merge_table.sh" | grep -v "grep" | wc -l)
    else
        break
    fi
done

sleep 10
if [[ "$users" != "" ]];then
  send_message "$users" "【小文件合并任务提醒】您的合并任务已经完成,列表: ${table_list}中的所有文件已经合并完成,起始时间:数据起始,结束时间:${end_dt}(不包含),请确认成功后务必手动删除${tmp_db_name}库中对应的临时表!"
fi
