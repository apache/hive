set hive.support.concurrency=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

set hive.optimize.metadataonly=true;

create table test1 (id int, val string) partitioned by (val2 string) STORED AS ORC TBLPROPERTIES ('transactional'='true');
describe formatted test1;

alter table test1 add partition (val2='foo');
alter table test1 add partition (val2='bar');
insert into test1 partition (val2='foo') values (1, 'abc');
insert into test1 partition (val2='bar') values (1, 'def');
delete from test1 where val2 = 'bar';

select '--> hive.optimize.metadataonly=true';
select distinct val2 from test1;


set hive.optimize.metadataonly=false;
select '--> hive.optimize.metadataonly=false';
select distinct val2 from test1;
