set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=exim_department_n6,exim_employee;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;

create table exim_department_n6 ( dep_id int) stored as textfile;
load data local inpath "../../data/files/test.dat" into table exim_department_n6;
dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/exim_department/test;
dfs -rmr target/tmp/ql/test/data/exports/exim_department;
export table exim_department_n6 to 'ql/test/data/exports/exim_department';
drop table exim_department_n6;

create database importer;
use importer;

set hive.security.authorization.enabled=true;
grant Create on database importer to user hive_test_user;
import from 'ql/test/data/exports/exim_department';

set hive.security.authorization.enabled=false;
select * from exim_department_n6;
drop table exim_department_n6;
drop database importer;
dfs -rmr target/tmp/ql/test/data/exports/exim_department;

