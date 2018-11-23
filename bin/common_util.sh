#!/bin/bash

function send_message()
{
  local msgURL='http://smsapi.in.autohome.com.cn/api/sms/send'
  local phoneNum=$1
  local msg="$2"
  local msg1="`date \"+%F %T\"`%0a${msg}"
  local msg2=${msg1:0:200}
  [[ "$msg2" != "$msg1" ]] && msg2=$msg2"...(未完)"
  local msg3=`echo "$msg2"|sed 's/ /%20/g;s/$/%0a/g'`
  local form="_appid=pv&mobile=${phoneNum}&message=${msg3}"
  curl -i -X GET "${msgURL}?${form}" >/dev/null 2>&1
}

function print_to_stdout()
{
  time=$(date "+%Y-%m-%d %H:%M:%S")
  if [[ $# != 2 ]];then
    echo -e "[INFO][${time}] $1" >&1
  else
    if [[ "$2" = "error" ]];then
      echo -e "[ERROR][${time}] $1" >&1
      if [[ "$users" != "" ]];then
        echo -e "[INFO][${time}] 开始发送短信通知用户: ${users}" >&1
        send_message "$users" "【小文件合并任务提醒】$1"
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