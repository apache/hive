set hive.acid.direct.insert.enabled=true;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.stats.autogather=true;

drop table if exists multi_test_text;
drop table if exists multi_test_acid;

create external table multi_test_text (a int, b int, c int) stored as textfile;

insert into multi_test_text values (1111, 11, 1111), (2222, 22, 1111), (3333, 33, 2222), (4444, 44, NULL), (5555, 55, NULL);

create table multi_test_acid (a int, b int) partitioned by (c int) stored as orc tblproperties('transactional'='true');

from multi_test_text a
insert overwrite table multi_test_acid partition (c=66)
select
 a.a,
 a.b
 where a.c is not null
insert overwrite table multi_test_acid partition (c=77)
select
 a.a,
 a.b
where a.c=1
insert overwrite table multi_test_acid partition (c=88)
select
 a.a,
 a.b
where a.c is null;

select * from multi_test_acid order by a;

drop table if exists multi_test_text;
drop table if exists multi_test_acid;