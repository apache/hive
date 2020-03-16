--! qt:disabled:Disabled in HIVE-21427
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
dfs -cp '../../data/files/rfc5424-hs2.log' ${hiveconf:hive.metastore.warehouse.dir}/logs2/dt=2019-03-21/ns=foo/app=hs2/2019-03-21-07-05_0.log;
dfs -mkdir -p ${hiveconf:hive.metastore.warehouse.dir}/logs2/dt=2019-03-22/ns=bar/app=hs2/;
dfs -cp '../../data/files/rfc5424-hs2-2.log' ${hiveconf:hive.metastore.warehouse.dir}/logs2/dt=2019-03-22/ns=bar/app=hs2/2019-03-22-01-05_0.log;

MSCK REPAIR TABLE logs2;

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecTezSummaryPrinter;
set hive.syslog.input.format.file.pruning=false;
-- before
select severity,count(*) from logs2 where ts between '2019-03-21 07:00:00.0' and '2019-03-21 07:06:00.0' group by severity;
-- middle
select severity,count(*) from logs2 where ts between '2019-03-21 07:06:00.0' and '2019-03-21 07:07:00.0' group by severity;
-- middle
select severity,count(*) from logs2 where ts between '2019-03-21 07:07:00.0' and '2019-03-21 07:08:00.0' group by severity;
-- after
select severity,count(*) from logs2 where ts between '2019-03-21 07:08:00.0' and '2019-03-21 08:08:00.0' group by severity;
-- all
select severity,count(*) from logs2 where ts between '2019-03-21 07:00:00.0' and '2019-03-21 08:00:00.0' group by severity;
-- all no filter
select severity,count(*) from logs2 where dt='2019-03-21' group by severity;

-- before
select severity,count(*) from logs2 where ts between '2019-03-22 01:00:00.0' and '2019-03-22 01:08:00.0' group by severity;
-- middle
select severity,count(*) from logs2 where ts between '2019-03-22 01:08:00.0' and '2019-03-22 01:09:00.0' group by severity;
-- after
select severity,count(*) from logs2 where ts between '2019-03-22 01:09:00.0' and '2019-03-22 01:10:00.0' group by severity;
-- all
select severity,count(*) from logs2 where ts between '2019-03-22 01:00:00.0' and '2019-03-22 02:00:00.0' group by severity;
-- all no filter
select severity,count(*) from logs2 where dt='2019-03-22' group by severity;

set hive.syslog.input.format.file.pruning=true;
-- before
select severity,count(*) from logs2 where ts between '2019-03-21 07:00:00.0' and '2019-03-21 07:06:00.0' group by severity;
-- middle
select severity,count(*) from logs2 where ts between '2019-03-21 07:06:00.0' and '2019-03-21 07:07:00.0' group by severity;
-- middle
select severity,count(*) from logs2 where ts between '2019-03-21 07:07:00.0' and '2019-03-21 07:08:00.0' group by severity;
-- after
select severity,count(*) from logs2 where ts between '2019-03-21 07:08:00.0' and '2019-03-21 08:08:00.0' group by severity;
-- all
select severity,count(*) from logs2 where ts between '2019-03-21 07:00:00.0' and '2019-03-21 08:00:00.0' group by severity;
-- all no filter
select severity,count(*) from logs2 where dt='2019-03-21' group by severity;

-- before
select severity,count(*) from logs2 where ts between '2019-03-22 01:00:00.0' and '2019-03-22 01:08:00.0' group by severity;
-- middle
select severity,count(*) from logs2 where ts between '2019-03-22 01:08:00.0' and '2019-03-22 01:09:00.0' group by severity;
-- after
select severity,count(*) from logs2 where ts between '2019-03-22 01:09:00.0' and '2019-03-22 01:10:00.0' group by severity;
-- all
select severity,count(*) from logs2 where ts between '2019-03-22 01:00:00.0' and '2019-03-22 02:00:00.0' group by severity;
-- all no filter
select severity,count(*) from logs2 where dt='2019-03-22' group by severity;

drop table logs2;
