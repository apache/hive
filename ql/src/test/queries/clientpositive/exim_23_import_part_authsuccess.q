set hive.mapred.mode=nonstrict;
set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=exim_department,exim_employee_n7;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;

create table exim_employee_n7 ( emp_id int comment "employee id") 	
	comment "employee table"
	partitioned by (emp_country string comment "two char iso code", emp_state string comment "free text")
	stored as textfile	
	tblproperties("creator"="krishna");
load data local inpath "../../data/files/test.dat" 
	into table exim_employee_n7 partition (emp_country="in", emp_state="tn");
dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/exim_employee/temp;
dfs -rmr target/tmp/ql/test/data/exports/exim_employee;
export table exim_employee_n7 to 'ql/test/data/exports/exim_employee';
drop table exim_employee_n7;

create database importer;
use importer;
create table exim_employee_n7 ( emp_id int comment "employee id") 	
	comment "employee table"
	partitioned by (emp_country string comment "two char iso code", emp_state string comment "free text")
	stored as textfile	
	tblproperties("creator"="krishna");

set hive.security.authorization.enabled=true;
grant Alter on table exim_employee_n7 to user hive_test_user;
grant Update on table exim_employee_n7 to user hive_test_user;
import from 'ql/test/data/exports/exim_employee';

set hive.security.authorization.enabled=false;
select * from exim_employee_n7;
dfs -rmr target/tmp/ql/test/data/exports/exim_employee;
drop table exim_employee_n7;
drop database importer;
