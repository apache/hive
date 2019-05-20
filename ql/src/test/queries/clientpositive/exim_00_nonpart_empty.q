set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=exim_department_n0,exim_employee;

create table exim_department_n0 ( dep_id int comment "department id") 	
	stored as textfile	
	tblproperties("creator"="krishna");
dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/exim_department/temp;
dfs -rmr target/tmp/ql/test/data/exports/exim_department;
export table exim_department_n0 to 'ql/test/data/exports/exim_department';
drop table exim_department_n0;

create database importer;
use importer;

import from 'ql/test/data/exports/exim_department';
describe extended exim_department_n0;
show table extended like exim_department_n0;
dfs -rmr target/tmp/ql/test/data/exports/exim_department;
select * from exim_department_n0;
drop table exim_department_n0;

drop database importer;
