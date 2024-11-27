set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.cli.print.header=true;

create table tmp1 (a int, b string) stored as orc TBLPROPERTIES ('transactional'='true');
create table tmp2 (a int, b string) stored as orc TBLPROPERTIES ('transactional'='true');
insert into table tmp1 values (3, "a");

explain cbo
select count(*) from tmp1;
select count(*) from tmp1;

explain cbo
select count(*) from tmp2;
select count(*) from tmp2;

explain cbo 
select * from tmp1 join tmp2 using(a);
select * from tmp1 join tmp2 using(a);

set hive.cbo.rule.exclusion.regex=;

explain cbo
select count(*) from tmp1;
select count(*) from tmp1;

explain cbo
select count(*) from tmp2;
select count(*) from tmp2;

explain cbo 
select * from tmp1 join tmp2 using(a);
select * from tmp1 join tmp2 using(a);
