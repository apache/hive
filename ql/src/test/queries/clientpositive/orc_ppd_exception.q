set hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.optimize.index.filter=true;
set hive.cbo.enable=false;

create table test_acid( i int, ts timestamp)
                      clustered by (i) into 2 buckets
                      stored as orc
                      tblproperties ('transactional'='true');
insert into table test_acid values (1, '2014-09-14 12:34:30');
delete from test_acid where ts = '2014-15-16 17:18:19.20';
select i,ts from test_acid where ts = '2014-15-16 17:18:19.20';
select i,ts from test_acid where ts <= '2014-09-14 12:34:30';

drop table test_acid;
set hive.exec.orc.delta.streaming.optimizations.enabled=true;

create table test_acid( i int, ts timestamp)
                      clustered by (i) into 2 buckets
                      stored as orc
                      tblproperties ('transactional'='true');
insert into table test_acid values (1, '2014-09-14 12:34:30');
delete from test_acid where ts = '2014-15-16 17:18:19.20';
select i,ts from test_acid where ts = '2014-15-16 17:18:19.20';
select i,ts from test_acid where ts <= '2014-09-14 12:34:30';

