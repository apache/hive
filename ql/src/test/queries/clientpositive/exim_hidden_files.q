set hive.mapred.mode=nonstrict;
set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=exim_department,exim_employee_n6;

create table exim_employee_n6 ( emp_id int) partitioned by (emp_country string);
load data local inpath "../../data/files/test.dat" into table exim_employee_n6 partition (emp_country="in");		

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/exim_employee_n6/emp_country=in/_logs;
dfs -touchz ${system:test.warehouse.dir}/exim_employee_n6/emp_country=in/_logs/job.xml;
export table exim_employee_n6 to 'ql/test/data/exports/exim_employee_n6';
drop table exim_employee_n6;

create database importer;
use importer;

import from 'ql/test/data/exports/exim_employee_n6';
describe formatted exim_employee_n6;
select * from exim_employee_n6;
dfs -rmr target/tmp/ql/test/data/exports/exim_employee_n6;
drop table exim_employee_n6;
drop database importer;
use default;
