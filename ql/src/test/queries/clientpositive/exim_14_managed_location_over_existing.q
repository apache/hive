set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=exim_department_n10,exim_employee;

create table exim_department_n10 ( dep_id int comment "department id") 	
	stored as textfile	
	tblproperties("creator"="krishna");
load data local inpath "../../data/files/test.dat" into table exim_department_n10;		
dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/exim_department/temp;
dfs -rmr target/tmp/ql/test/data/exports/exim_department;
export table exim_department_n10 to 'ql/test/data/exports/exim_department';
drop table exim_department_n10;

create database importer;
use importer;

dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/tablestore/exim_department/temp;
dfs -rmr target/tmp/ql/test/data/tablestore/exim_department;

create table exim_department_n10 ( dep_id int comment "department id") 	
	stored as textfile
	location 'ql/test/data/tablestore/exim_department'
	tblproperties("creator"="krishna");
import table exim_department_n10 from 'ql/test/data/exports/exim_department'
	location 'ql/test/data/tablestore/exim_department';
describe extended exim_department_n10;
dfs -rmr target/tmp/ql/test/data/exports/exim_department;
select * from exim_department_n10;
dfs -rmr target/tmp/ql/test/data/tablestore/exim_department;
select * from exim_department_n10;	
drop table exim_department_n10;

drop database importer;
