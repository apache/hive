--! qt:replace:/(\s+totalSize\s+)\S+(\s+)/$1#Masked#/
-- SORT_QUERY_RESULTS

set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.explain.user=false;
set hive.merge.cardinality.check=true;

create table t(a int, b int) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
create table upd_t(a int, b int) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='false');

desc formatted t;

insert into t values (1,1);
insert into upd_t values (1,1),(2,2);

desc formatted t;

explain merge into t as t using upd_t as u ON t.a = u.a 
WHEN MATCHED THEN UPDATE SET b = 99
WHEN NOT MATCHED THEN INSERT VALUES(u.a, u.b);

merge into t as t using upd_t as u ON t.a = u.a 
WHEN MATCHED THEN UPDATE SET b = 99
WHEN NOT MATCHED THEN INSERT VALUES(u.a, u.b);

-- merge could keep track of inserts
select assert_true(count(1) = 2) from t group by a>-1;
-- rownum is 2
desc formatted t;

merge into t as t using upd_t as u ON t.a = u.a 
WHEN MATCHED THEN DELETE
WHEN NOT MATCHED THEN INSERT (a, b) VALUES(u.a, u.b);

select assert_true(count(1) = 0) from t group by a>-1;
-- rownum is 0; because the orc writer can keep track of delta
desc formatted t;

create table t2(a int, b int, c int default 1) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
create table upd_t2_1(a int, b int) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='false');
create table upd_t2_2(a int, b int) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='false');
create table upd_t2_3(a int, b int) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='false');
create table upd_t2_4(a int, b int) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='false');

desc formatted t2;

insert into t2 (a, b) values (1,1), (3,3), (5,5), (7,7);
insert into upd_t2_1 values (1,1),(2,2);
insert into upd_t2_2 values (3,3),(4,4);
insert into upd_t2_3 values (5,5),(6,6);
insert into upd_t2_4 values (7,7),(8,8);

explain merge into t2 as t using upd_t2_1 as u ON t.a = u.a
WHEN MATCHED THEN UPDATE SET b = 99
WHEN NOT MATCHED THEN INSERT VALUES(u.a, u.b, default);

merge into t2 as t using upd_t2_1 as u ON t.a = u.a
WHEN MATCHED THEN UPDATE SET b = 99
WHEN NOT MATCHED THEN INSERT VALUES(u.a, u.b, default);

explain merge into t2 as t using upd_t2_2 as u ON t.a = u.a
WHEN MATCHED THEN UPDATE SET b = 98
WHEN NOT MATCHED THEN INSERT (a, b) VALUES(u.a, u.b);

merge into t2 as t using upd_t2_2 as u ON t.a = u.a
WHEN MATCHED THEN UPDATE SET b = 98
WHEN NOT MATCHED THEN INSERT (a, b) VALUES(u.a, u.b);

explain merge into t2 as t using upd_t2_3 as u ON t.a = u.a
WHEN MATCHED THEN UPDATE SET b = 97
WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES(u.a, u.b, default);

merge into t2 as t using upd_t2_3 as u ON t.a = u.a
WHEN MATCHED THEN UPDATE SET b = 97
WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES(u.a, u.b, default);

explain merge into t2 as t using upd_t2_4 as u ON t.a = u.a
WHEN MATCHED THEN UPDATE SET b = 96
WHEN NOT MATCHED THEN INSERT (b, c, a) VALUES(u.b, default, u.a);

merge into t2 as t using upd_t2_4 as u ON t.a = u.a
WHEN MATCHED THEN UPDATE SET b = 96
WHEN NOT MATCHED THEN INSERT (b, c, a) VALUES(u.b, default, u.a);

select * from t2;

create table t3(a int, b int default 1) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
create table upd_t3(a int, b int) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

insert into t3 values (1,2), (2,4);
insert into upd_t3 values (1,3), (3,5);

explain merge into t3 as t using upd_t3 as u ON t.a = u.a
WHEN MATCHED THEN DELETE
WHEN NOT MATCHED THEN INSERT (b, a) VALUES(default, u.b);

merge into t3 as t using upd_t3 as u ON t.a = u.a
WHEN MATCHED THEN DELETE
WHEN NOT MATCHED THEN INSERT (b, a) VALUES(default, u.b);

select * from t3;

create table t4(a int, b int default 1) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
create table upd_t4(a int, b int) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

insert into t4 values (1,2), (2,4);
insert into upd_t4 values (1,3), (3,5);

explain merge into t4 as t using upd_t4 as u ON t.a = u.a
WHEN MATCHED THEN UPDATE SET b = default
WHEN NOT MATCHED THEN INSERT (b, a) VALUES(default, u.b);

merge into t4 as t using upd_t4 as u ON t.a = u.a
WHEN MATCHED THEN UPDATE SET b = default
WHEN NOT MATCHED THEN INSERT (b, a) VALUES(default, u.b);

select * from t4;

