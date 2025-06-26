set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.acid.direct.insert.enabled=true;

drop table if exists full_acid;

set mapreduce.job.reduces=7;

create external table ext(a int) stored as textfile;
insert into table ext values(1),(2),(3),(4),(5),(6),(7), (8), (9), (12);
create table full_acid(a int) stored as orc tblproperties("transactional"="true");

insert into table full_acid select * from ext where a != 3 and a <=7 group by a;
insert into table full_acid select * from ext where a>7 group by a;

set mapreduce.job.reduces=1;
delete from full_acid where a in (2, 12);