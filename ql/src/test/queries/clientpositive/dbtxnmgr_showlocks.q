-- Mask the enqueue time which is based on current time
--! qt:replace:/(initiated\s+---\s+---\s+)[0-9]*(\s+---)/$1#Masked#$2/
-- Mask the hostname in show compaction
--! qt:replace:/(---\s+)[\S]*(\s+manual)/$1#Masked#$2/
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

show locks;

show locks extended;

show locks default;

explain show transactions;
show transactions;

create table partitioned_acid_table (a int, b int) partitioned by (p string) clustered by (a) into 2 buckets stored as orc tblproperties ('transactional'='true');

explain show locks database default;
show locks database default;

show locks hive.default.partitioned_acid_table;

show locks database hive.default;

show locks partitioned_acid_table;

show locks partitioned_acid_table extended;

show locks partitioned_acid_table partition (p='abc');

explain show locks partitioned_acid_table partition (p='abc') extended;
show locks partitioned_acid_table partition (p='abc') extended;

insert into partitioned_acid_table partition(p='abc') values(1,2);

alter table partitioned_acid_table partition(p='abc') compact 'minor';

explain show compactions;
show compactions;

drop table partitioned_acid_table;

-- CREATE a new catalog with comment
CREATE CATALOG testcatalog LOCATION '/tmp/testcatalog' COMMENT 'Hive testing catalog';

-- Switch the catalog from hive to 'testcatalog'
SET CATALOG testcatalog;

-- Check the current catalog, should be testcatalog.
select current_catalog();

-- CREATE DATABASE in new catalog testcat by catalog.db pattern
CREATE DATABASE testcatalog.testdb;

-- CREATE TABLE under the new database
CREATE TABLE partitioned_newcatalog_table (a int, b int) PARTITIONED BY (c string);

show locks;

show locks extended;

show locks database testdb;

show locks testcatalog.testdb.partitioned_newcatalog_table;

show locks database testcatalog.testdb;

show locks partitioned_newcatalog_table;

show locks partitioned_acid_table extended;

DROP DATABASE testcatalog.testdb;
SET CATALOG hive;