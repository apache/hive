set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

CREATE EXTERNAL TABLE logs(
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
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.log.syslog.SyslogSerDe'
STORED AS TEXTFILE;

load data local inpath '../../data/files/rfc5424-hs2.log' into table logs;
load data local inpath '../../data/files/rfc5424-hs2-2.log' into table logs;

select count(*) from logs where severity="ERROR";
select count(*) from logs;
select count(*) from logs where unmatched is null;
select count(*) from logs where unmatched is not null;
select severity,count(*) from logs group by severity order by severity;
select severity,count(*) from logs where ts between "2019-03-21 07:00:00" and "2019-03-21 07:06:00" group by severity;
select severity,count(*) from logs where ts between "2019-03-21 07:06:00" and "2019-03-21 07:07:00" group by severity;
select severity,count(*) from logs where ts between "2019-03-21 07:07:00" and "2019-03-21 07:08:00" group by severity;
select severity,count(*) from logs where ts between "2019-03-21 07:08:00" and "2019-03-21 08:08:00" group by severity;
select severity,count(*) from logs where ts between "2019-03-22 01:00:00" and "2019-03-21 01:08:00" group by severity;
select severity,count(*) from logs where ts between "2019-03-22 01:08:00" and "2019-03-21 01:09:00" group by severity;
select severity,count(*) from logs where ts between "2019-03-22 01:09:00" and "2019-03-21 01:10:00" group by severity;
select ts,severity,structured_data["thread"],structured_data["class"] from logs where severity="ERROR" ORDER BY ts;
select ts,severity,structured_data["thread"],structured_data["class"],app_name from logs where severity="WARN" ORDER BY ts;
select decode(unmatched, 'UTF-8') from logs where unmatched is not null limit 10;

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

dfs -mkdir -p ${hiveconf:hive.metastore.warehouse.dir}/logs2/dt=2019-01-03/ns=foo/app=hs2/;
dfs -cp '../../data/files/rfc5424-hs2.log' ${hiveconf:hive.metastore.warehouse.dir}/logs2/dt=2019-01-03/ns=foo/app=hs2/2019-03-21-07-05.log;
dfs -mkdir -p ${hiveconf:hive.metastore.warehouse.dir}/logs2/dt=2019-11-23/ns=bar/app=hs2/;
dfs -cp '../../data/files/rfc5424-hs2.log' ${hiveconf:hive.metastore.warehouse.dir}/logs2/dt=2019-11-23/ns=bar/app=hs2/2019-02-22-01-05.log;

MSCK REPAIR TABLE logs2;

select severity,count(*) from logs2 group by severity order by severity;
select count(*) from logs2 where severity="INFO" and dt='2019-01-03';
select count(*) from logs2 where severity="INFO" and ns='foo';
select count(*) from logs2 where severity="INFO" and dt='2019-11-23';
select count(*) from logs2 where severity="INFO" and ns='bar';
select count(*) from logs2 where severity="INFO";
select count(*) from logs2 where severity="INFO" and app='hs2';
select count(*) from logs2 where severity="INFO" and app='non-existent';
select count(*) from logs2 where ns='foo';
select count(*) from logs2 where ns='bar';
select count(*) from logs2 where unmatched is null and dt between '2019-01-03' and '2019-11-23';
select count(*) from logs2 where unmatched is not null;
select ts,severity,structured_data["thread"],structured_data["class"] from logs2 where severity="ERROR" order by ts;
select ts,severity,structured_data["thread"],structured_data["class"],app_name from logs2 where severity="WARN" order by ts;
select decode(unmatched, 'UTF-8') from logs where unmatched is not null limit 10;

drop table logs;
drop table logs2;

dfs -rm -r ${hiveconf:hive.metastore.warehouse.dir}/logs2/;
