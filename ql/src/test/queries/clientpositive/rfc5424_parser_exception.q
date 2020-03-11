set hive.fetch.task.conversion=none;

set time zone UTC;

-- SORT_QUERY_RESULTS

CREATE EXTERNAL TABLE logs2(
facility STRING,
severity STRING,
version STRING,
ts TIMESTAMP,
hostname STRING,
app_name STRING,
proc_id STRING,
msg_id STRING,
structured_data MAP<STRING,STRING>,
msg BINARY,
unmatched BINARY
)
PARTITIONED BY(dt DATE,ns STRING,app STRING)
STORED BY 'org.apache.hadoop.hive.ql.log.syslog.SyslogStorageHandler'
LOCATION "${hiveconf:hive.metastore.warehouse.dir}/logs2";

dfs -mkdir -p ${hiveconf:hive.metastore.warehouse.dir}/logs2/dt=2019-03-21/ns=foo/app=hs2/;
dfs -cp '../../data/files/rfc5424-hs2-exception.log' ${hiveconf:hive.metastore.warehouse.dir}/logs2/dt=2019-03-21/ns=foo/app=hs2/2019-03-21-07-05_0.log;

MSCK REPAIR TABLE logs2;

-- 2 rows
select count(*) as msg from logs2 where ts is not null;
-- 0 rows (multi-line exception should be part of first row, so no umatched is expected)
select count(*) as msg from logs2 where ts is null;
select length(decode(msg,'UTF-8')) as msg from logs2;
-- qtest output escapes exceptions with patterns "*Caused by*" etc. so we replace before printing exception
select regexp_replace(regexp_replace(decode(msg,'UTF-8'), "at ", "at-"), "Caused by", "Caused-by") as msg from logs2;

drop table logs2;

dfs -rm -r ${hiveconf:hive.metastore.warehouse.dir}/logs2/;
