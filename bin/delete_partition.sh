#!/bin/bash

if [[ $# -ne 2 ]];then
  echo -e "执行方式: $0 <参数1> <参数2> \n
  参数说明:
  \t参数1: 必填,表列表
  \t参数2: 临时库名 \n
  注意：脚本日志会重定向到标准输出中，HIVE任务相关的日志会重定向到标准错误中"
  exit 1
fi

if [[ $# -ne 2 ]];then
  echo -e "执行方式: $0 <参数1> <参数2> \n
  参数说明:
  \t参数1: 必填,表列表
  \t参数2: 临时库名 \n
  注意：脚本日志会重定向到标准输出中，HIVE任务相关的日志会重定向到标准错误中"
  exit 1
fi

source ./common_util.sh
table_list=$1
tmp_db_name=$2

# 校验列表文件是否存在
if [[ ! -f "${table_list}" ]];then
  print_to_stdout "列表文件: ${table_list} 不存在!" "error"
  exit 1
fi

if [[ ! -f "/tmp/drop_table_20181206.txt" ]];then
    tables=($(cat ${table_list}))
    for table_name in ${tables[@]}
    do
        new_table_name="${tmp_db_name}.${table_name}"
        dts=($(hive -e "show partitions ${new_table_name}" 2>/dev/null | awk -F'/' '{if(index($1, "dt") != 0) print $1; else print $2;}' | sort -u | awk -F'=' '{print $2}'))
        dts_len=$((${#dts[@]}-3))

        for ((i=0; i<$dts_len; i ++))
        do
            print_to_stdout "开始删除表: ${new_table_name} 的分区: dt=${dts[i]}"
            echo "ALTER TABLE ${new_table_name} DROP IF EXISTS PARTITION (dt='"${dts[i]}"');" >> /tmp/drop_table_20181206.txt
        done
    done
fi

print_to_stdout "开始调用HIVE执行删除分区!"
#hive -f /tmp/drop_table_20181206.txt
#if [[ $? -ne 0 ]];then
#    print_to_stdout "调用hive删除表分区失败！" "error"
#    exit 1
#fi

#rm /tmp/drop_table_20181206.txt