
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop table if exists smt1_tab1;
drop table if exists smt1_tab2;

create table smt1_tab1 (c1 string, c2 string) stored as textfile;
load data local inpath '../../data/files/kv1.txt' into table smt1_tab1;
select * from smt1_tab1 where c1 = '0';

create table smt1_tab2 (c1 string, c2 string) stored as textfile;
insert into table smt1_tab2 select * from smt1_tab1;
select * from smt1_tab2 where c1 = '0';

-- After this point, managed non-transactional table not valid
set metastore.strict.managed.tables=true;

-- Setting to external table should allow table to be usable
alter table smt1_tab1 set tblproperties('EXTERNAL'='TRUE');
select * from smt1_tab1 where c1 = '0';

alter table smt1_tab2 set tblproperties('EXTERNAL'='TRUE');
select * from smt1_tab2 where c1 = '0';

-- Setting to managed insert-only transactional table should allow table to be usable
alter table smt1_tab1 set tblproperties('EXTERNAL'='FALSE', 'transactional'='true', 'transactional_properties'='insert_only');
select * from smt1_tab1 where c1 = '0';

alter table smt1_tab2 set tblproperties('EXTERNAL'='FALSE', 'transactional'='true', 'transactional_properties'='insert_only');
select * from smt1_tab2 where c1 = '0';

-- Temp table still works
create temporary table smt1_tmp (c1 string, c2 string) stored as orc;
insert into table smt1_tmp values ('123', '456');

select * from smt1_tmp;

select c1, count(*) from smt1_tmp group by c1;

