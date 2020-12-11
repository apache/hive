set hive.test.mode=true;
set hive.repl.include.external.tables=false;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=exim_department_n4,exim_employee;

dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/tablestore/exim_department/temp;
dfs -rmr target/tmp/ql/test/data/tablestore/exim_department;
create external table exim_department_n4 ( dep_id int comment "department id") 	
	stored as textfile	
	location 'ql/test/data/tablestore/exim_department'
	tblproperties("creator"="krishna");
load data local inpath "../../data/files/test.dat" into table exim_department_n4;		
dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/exim_department/temp;
dfs -rmr target/tmp/ql/test/data/exports/exim_department;
export table exim_department_n4 to 'ql/test/data/exports/exim_department';
drop table exim_department_n4;
dfs -rmr target/tmp/ql/test/data/tablestore/exim_department;

create database importer;
use importer;

import from 'ql/test/data/exports/exim_department';
describe extended exim_department_n4;
select * from exim_department_n4;
drop table exim_department_n4;
dfs -rmr target/tmp/ql/test/data/exports/exim_department;

drop database importer;
