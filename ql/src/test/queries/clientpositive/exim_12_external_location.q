set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=exim_department,exim_employee;

create table exim_department ( dep_id int comment "department id") 	
	stored as textfile	
	tblproperties("creator"="krishna");
load data local inpath "../data/files/test.dat" into table exim_department;		
dfs -mkdir ../build/ql/test/data/exports/exim_department/temp;
dfs -rmr ../build/ql/test/data/exports/exim_department;
export table exim_department to 'ql/test/data/exports/exim_department';
drop table exim_department;

create database importer;
use importer;

dfs -mkdir ../build/ql/test/data/tablestore/exim_department/temp;
dfs -rmr ../build/ql/test/data/tablestore/exim_department;

import external table exim_department from 'ql/test/data/exports/exim_department' 
	location 'ql/test/data/tablestore/exim_department';
describe extended exim_department;
dfs -rmr ../build/ql/test/data/exports/exim_department;
select * from exim_department;
dfs -rmr ../build/ql/test/data/tablestore/exim_department;
select * from exim_department;
drop table exim_department;

drop database importer;
