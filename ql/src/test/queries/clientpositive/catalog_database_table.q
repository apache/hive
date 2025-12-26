-- SORT_QUERY_RESULTS

create catalog testcat location '/tmp/testcat' comment 'Hive test catalog';

-- create database in new catalog testcat by catalog.db pattern
create database testcat.testdb1;

-- switch current db to testcat.testdb1
use testcat.testdb1;

-- create tbl in the current db testcat.testdb1
create table test1(id int);

-- create tbl in db testcat.testdb1 by cat.db.tbl syntax
create table testcat.testdb1.test2(id int);
