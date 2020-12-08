-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop table if exists target;
drop table if exists source;

create table target (a int, b int) partitioned by (p int, q int) clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true');
create table source (a1 int, b1 int, p1 int, q1 int) clustered by (a1) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true');
insert into target partition(p,q) values (1,2,1,2), (3,4,1,2), (5,6,1,3), (7,8,2,2);
insert into source values (1,2,1,2), (3,4,1,2), (5,6,1,3), (7,8,2,2), (111,111,111,111);

select * from source;
select * from target;

-- the intent here is to record the set of ReadEntity and WriteEntity objects for these 2 update statements
update target set b = 1 where q in (select s.q1 from source s where s.a1=5);
select * from target;

update source set b1 = 1 where p1 in (select t.q from target t where t.p=2);
select * from source;

drop table if exists target;
drop table if exists source;

create table target (a int, b int) partitioned by (p int, q int) clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true');
create table source (a1 int, b1 int, p1 int, q1 int) clustered by (a1) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true');
insert into target partition(p,q) values (1,2,1,2), (3,4,1,2), (5,6,1,3), (7,8,2,2);
insert into source values (1,2,1,2), (3,4,1,2), (5,6,1,3), (7,8,2,2), (111,111,111,111);

-- the extra predicates in when matched clause match 1 partition
merge into target t using source s on t.a = s.a1 
when matched and p = 1 and q = 2 then update set b = 1 
when matched and p = 2 and q = 2 then delete 
when not matched and a1 > 100 then insert values(s.a1,s.b1,s.p1, s.q1);

drop table if exists target;
drop table if exists source;
