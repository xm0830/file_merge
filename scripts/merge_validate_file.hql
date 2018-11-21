set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=2;

set mapreduce.job.queuename=${queue_name};

select
  count(1)
from
  (
    select
      md5(concat(${hivevar:cols})) as md5_value,
      count(1) as md5_count
    from
      (
        select
          ${hivevar:validate_cols}
        from
          ${hivevar:full_table_name}
        where
          dt = '${hivevar:dt}'
      ) as a
    group by
      md5(concat(${hivevar:cols}))
  ) as c full outer join (
    select
      md5(concat(${hivevar:cols})) as md5_value,
      count(1) as md5_count
    from
      (
        select
          ${hivevar:validate_cols}
        from
          ${hivevar:tmp_db_name}.${hivevar:table_name}
        where
          dt = '${hivevar:dt}'
      ) as b
    group by
      md5(concat(${hivevar:cols}))
  ) as d on c.md5_value = d.md5_value
where
  c.md5_count != d.md5_count or c.md5_count is null or d.md5_count is null;