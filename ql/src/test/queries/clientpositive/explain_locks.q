set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

explain locks drop table test_explain_locks;
explain locks create table test_explain_locks (a int, b int);
drop table if exists target;
drop table if exists source;

create table target (a int, b int) partitioned by (p int, q int) clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true');
create table source (a1 int, b1 int, p1 int, q1 int) clustered by (a1) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true');
insert into target partition(p,q) values (1,2,1,2), (3,4,1,2), (5,6,1,3), (7,8,2,2);

-- the intent here is to record the set of ReadEntity and WriteEntity objects for these 2 update statements
explain locks update target set b = 1 where p in (select t.q1 from source t where t.a1=5);

explain locks update source set b1 = 1 where p1 in (select t.q from target t where t.p=2);

explain formatted locks update source set b1 = 1 where p1 in (select t.q from target t where t.p=2);

-- the extra predicates in when matched clause match 1 partition
explain locks merge into target t using source s on t.a = s.a1 when matched and p = 1 and q = 2 then update set b = 1 when matched and p = 2 and q = 2 then delete when not matched and a1 > 100 then insert values(s.a1,s.b1,s.p1, s.q1);