set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=exim_department_n5,exim_employee;

create table exim_department_n5 ( dep_id int comment "department id") 	
	stored as textfile	
	tblproperties("creator"="krishna");
load data local inpath "../../data/files/test.dat" into table exim_department_n5;
dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/exim_department/temp;
dfs -rmr target/tmp/ql/test/data/exports/exim_department;
export table exim_department_n5 to 'ql/test/data/exports/exim_department';
drop table exim_department_n5;

create database importer;
use importer;

create table exim_department_n5 ( dep_id int comment "department identifier") 	
	stored as textfile	
	tblproperties("maker"="krishna");
import from 'ql/test/data/exports/exim_department';
describe extended exim_department_n5;
select * from exim_department_n5;
drop table exim_department_n5;
dfs -rmr target/tmp/ql/test/data/exports/exim_department;

drop database importer;
