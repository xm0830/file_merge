set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions=5000;
set hive.exec.max.dynamic.partitions.pernode=5000;
set mapred.max.split.size=512000000;
set mapred.min.split.size.per.node=256000000;
set mapred.min.split.size.per.rack=256000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;

set mapreduce.job.queuename=${queue_name};

insert overwrite table ${hivevar:tmp_db_name}.${hivevar:table_name} partition (${hivevar:partition_cols}) select ${hivevar:cols} from ${hivevar:full_table_name} where dt='${hivevar:dt}'
