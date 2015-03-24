
DROP TABLE Employee_Part;

CREATE TABLE Employee_Part(employeeID int, employeeName String) partitioned by (employeeSalary double)
row format delimited fields terminated by '|'  stored as textfile;

LOAD DATA LOCAL INPATH "../../data/files/employee.dat" INTO TABLE Employee_Part partition(employeeSalary=2000.0);
LOAD DATA LOCAL INPATH "../../data/files/employee.dat" INTO TABLE Employee_Part partition(employeeSalary=4000.0);

explain 
analyze table Employee_Part partition (employeeSalary=2000.0) compute statistics for columns employeeID;
explain extended
analyze table Employee_Part partition (employeeSalary=2000.0) compute statistics for columns employeeID;
analyze table Employee_Part partition (employeeSalary=2000.0) compute statistics for columns employeeID;

explain 
analyze table Employee_Part partition (employeeSalary=4000.0) compute statistics for columns employeeID;
explain extended
analyze table Employee_Part partition (employeeSalary=4000.0) compute statistics for columns employeeID;
analyze table Employee_Part partition (employeeSalary=4000.0) compute statistics for columns employeeID;

explain 
analyze table Employee_Part partition (employeeSalary=2000.0) compute statistics for columns;
analyze table Employee_Part partition (employeeSalary=2000.0) compute statistics for columns;

describe formatted Employee_Part.employeeID   partition (employeeSalary=2000.0);
describe formatted Employee_Part.employeeName partition (employeeSalary=2000.0);

explain 
analyze table Employee_Part  compute statistics for columns;
analyze table Employee_Part  compute statistics for columns;

describe formatted Employee_Part.employeeID partition(employeeSalary=2000.0);
describe formatted Employee_Part.employeeID partition(employeeSalary=4000.0);

set hive.analyze.stmt.collect.partlevel.stats=false;
explain 
analyze table Employee_Part  compute statistics for columns;
analyze table Employee_Part  compute statistics for columns;

describe formatted Employee_Part.employeeID;

set hive.analyze.stmt.collect.partlevel.stats=true;

create database if not exists dummydb;

use dummydb;

analyze table default.Employee_Part partition (employeeSalary=2000.0) compute statistics for columns;

describe formatted default.Employee_Part employeeID   partition (employeeSalary=2000.0);

analyze table default.Employee_Part  compute statistics for columns;

use default;

drop database dummydb;

