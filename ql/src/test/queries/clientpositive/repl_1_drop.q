set hive.test.mode=true;
set hive.test.mode.prefix=;

create table repl_employee ( emp_id int comment "employee id")
	comment "employee table"
	partitioned by (emp_country string comment "two char iso code", emp_state string comment "free text")
	stored as textfile
	tblproperties("repl.last.id"="43");

load data local inpath "../../data/files/test.dat"
	into table repl_employee partition (emp_country="us", emp_state="ca");

show partitions repl_employee;
show table extended like repl_employee;

drop table repl_employee for replication('33');

-- drop 33 => table does not get dropped, but ca will be

show partitions repl_employee;
show table extended like repl_employee;

load data local inpath "../../data/files/test.dat"
	into table repl_employee partition (emp_country="us", emp_state="ak");

show partitions repl_employee;

drop table repl_employee for replication('');

-- drop '' => ptns would be dropped, but not tables

show partitions repl_employee;
show table extended like repl_employee;

drop table repl_employee for replication('49');

-- table and ptns should have been dropped, so next create can succeed

create table repl_employee ( emp_id int comment "employee id")
	comment "employee table"
	partitioned by (emp_country string comment "two char iso code", emp_state string comment "free text")
	stored as textfile;

-- created table without a repl.last.id

load data local inpath "../../data/files/test.dat"
	into table repl_employee partition (emp_country="us", emp_state="ca");
load data local inpath "../../data/files/test.dat"
	into table repl_employee partition (emp_country="us", emp_state="ak");
load data local inpath "../../data/files/test.dat"
	into table repl_employee partition (emp_country="us", emp_state="wa");

show partitions repl_employee;
show table extended like repl_employee;

alter table repl_employee drop partition (emp_country="us", emp_state="ca");
alter table repl_employee drop partition (emp_country="us", emp_state="wa") for replication('59');

-- should have dropped ca, wa

show partitions repl_employee;
show table extended like repl_employee;

alter table repl_employee set tblproperties ("repl.last.id" = "42");

alter table repl_employee drop partition (emp_country="us", emp_state="ak");

-- should have dropped ak

show partitions repl_employee;
show table extended like repl_employee;

drop table repl_employee;

-- should drop the whole table, and this can be verified by trying to create another table with the same name

create table repl_employee( a string);

show table extended like repl_employee;

drop table repl_employee;



