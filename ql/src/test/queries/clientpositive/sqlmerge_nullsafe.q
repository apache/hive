set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table t_target(a int, b string, c int)stored as orc TBLPROPERTIES ('transactional'='true');
create table t_source(a int, b string, c int);


insert into t_target values (1, 'match', 50), (2, 'not match', 51), (3, 'delete', 55), (4, 'not delete null', null), (null, 'match null', 56);
insert into t_source values (1, 'match', 50), (22, 'not match', 51), (3, 'delete', 55), (4, 'not delete null', null), (null, 'match null', 56);


explain
merge into t_target as t using t_source src ON t.a <=> src.a
when matched and t.c > 50 THEN DELETE
when matched then update set b = concat(t.b, ' Merged'), c = t.c + 10
when not matched then insert values (src.a, concat(src.b, ' New'), src.c);

merge into t_target as t using t_source src ON t.a <=> src.a
when matched and t.c > 50 THEN DELETE
when matched then update set b = concat(t.b, ' Merged'), c = t.c + 10
when not matched then insert values (src.a, concat(src.b, ' New'), src.c);

select * from t_target;
