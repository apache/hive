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
WHEN NOT MATCHED THEN INSERT VALUES(u.a, u.b);


select assert_true(count(1) = 0) from t group by a>-1;
-- rownum is 0; because the orc writer can keep track of delta
desc formatted t;

