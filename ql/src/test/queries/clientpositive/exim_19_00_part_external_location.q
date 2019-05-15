set hive.mapred.mode=nonstrict;
set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=exim_department,exim_employee_n2;

create table exim_employee_n2 ( emp_id int comment "employee id") 	
	comment "employee table"
	partitioned by (emp_country string comment "two char iso code", emp_state string comment "free text")
	stored as textfile	
	tblproperties("creator"="krishna");
load data local inpath "../../data/files/test.dat" 
	into table exim_employee_n2 partition (emp_country="in", emp_state="tn");	
load data local inpath "../../data/files/test2.dat" 
	into table exim_employee_n2 partition (emp_country="in", emp_state="ka");	
dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/exim_employee/temp;
dfs -rmr target/tmp/ql/test/data/exports/exim_employee;
export table exim_employee_n2 to 'ql/test/data/exports/exim_employee';
drop table exim_employee_n2;

create database importer;
use importer;

dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/tablestore/exim_employee/temp;
dfs -rmr target/tmp/ql/test/data/tablestore/exim_employee;

import external table exim_employee_n2 
	from 'ql/test/data/exports/exim_employee'
	location 'ql/test/data/tablestore/exim_employee';
describe extended exim_employee_n2;	
show table extended like exim_employee_n2;
show table extended like exim_employee_n2 partition (emp_country="in", emp_state="tn");
show table extended like exim_employee_n2 partition (emp_country="in", emp_state="ka");
dfs -rmr target/tmp/ql/test/data/exports/exim_employee;
select * from exim_employee_n2;
dfs -rmr target/tmp/ql/test/data/tablestore/exim_employee;
select * from exim_employee_n2;
drop table exim_employee_n2;

drop database importer;
