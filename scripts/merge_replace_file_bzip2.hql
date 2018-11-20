set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set mapred.max.split.size=512000000;
set mapred.min.split.size.per.node=256000000;
set mapred.min.split.size.per.rack=256000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;

set hive.exec.compress.output=true;  
set mapred.output.compress=true;  
set mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;  
set io.compression.codecs=org.apache.hadoop.io.compress.BZip2Codec;

set mapreduce.job.queuename=${queue_name};

insert overwrite table ${hivevar:full_table_name} partition (${hivevar:partition_cols}) select ${hivevar:cols} from ${hivevar:tmp_db_name}.${hivevar:table_name} where dt='${hivevar:dt}'
