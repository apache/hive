set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=exim_department_n1,exim_employee;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;

create table exim_department_n1 ( dep_id int) stored as textfile;
load data local inpath "../../data/files/test.dat" into table exim_department_n1;
dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/exim_department/temp;
dfs -rmr target/tmp/ql/test/data/exports/exim_department;
export table exim_department_n1 to 'ql/test/data/exports/exim_department';
drop table exim_department_n1;

create database importer;
use importer;

create table exim_department_n1 ( dep_id int) stored as textfile;
set hive.security.authorization.enabled=true;
grant Alter on table exim_department_n1 to user hive_test_user;
grant Update on table exim_department_n1 to user hive_test_user;
import from 'ql/test/data/exports/exim_department';

set hive.security.authorization.enabled=false;
select * from exim_department_n1;
drop table exim_department_n1;
drop database importer;
dfs -rmr target/tmp/ql/test/data/exports/exim_department;

