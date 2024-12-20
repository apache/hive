set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.create.as.external.legacy=true;

-- When hive.create.as.external.legacy is true, the tables created with
-- 'managed' or 'transactional' are ACID tables but the tables create
-- without 'managed' and 'transactional' are non-ACID tables.
-- Note: managed non-ACID tables are allowed because tables are not
-- transformed when hive.in.test is true.

-- Create tables with 'transactional'. These tables have table property
-- 'transactional'='true'
create transactional table test11 as select 1;
show create table test11;
describe formatted test11;

create transactional table test12 as select * from test11;
show create table test12;
describe formatted test12;

-- Create tables with 'managed'. These tables have table property
-- 'transactional'='true'
create managed table test21 as select 1;
show create table test21;
describe formatted test21;

create managed table test22 as select * from test21;
show create table test22;
describe formatted test22;

-- Create tables without the keyword. These tables doesn't have table 
-- property 'transactional'
create table test31 as select 1;
show create table test31;
describe formatted test31;

create table test32 as select * from test31;
show create table test32;
describe formatted test32;

-- create table with empty characters within quotes
create table ` default`.` table41`(i int);
show create table ` default`.` table41`;
describe formatted ` default`.` table41`;
