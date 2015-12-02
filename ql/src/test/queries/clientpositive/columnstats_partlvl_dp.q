set hive.mapred.mode=nonstrict;
DROP TABLE Employee_Part;

CREATE TABLE Employee_Part(employeeID int, employeeName String) partitioned by (employeeSalary double, country string)
row format delimited fields terminated by '|'  stored as textfile;

LOAD DATA LOCAL INPATH "../../data/files/employee.dat"  INTO TABLE Employee_Part partition(employeeSalary='2000.0', country='USA');
LOAD DATA LOCAL INPATH "../../data/files/employee2.dat" INTO TABLE Employee_Part partition(employeeSalary='2000.0', country='UK');
LOAD DATA LOCAL INPATH "../../data/files/employee2.dat" INTO TABLE Employee_Part partition(employeeSalary='3000.0', country='USA');
LOAD DATA LOCAL INPATH "../../data/files/employee2.dat" INTO TABLE Employee_Part partition(employeeSalary='4000.0', country='USA');
LOAD DATA LOCAL INPATH "../../data/files/employee2.dat" INTO TABLE Employee_Part partition(employeeSalary='3500.0', country='UK');
LOAD DATA LOCAL INPATH "../../data/files/employee.dat"  INTO TABLE Employee_Part partition(employeeSalary='3000.0', country='UK');

-- dynamic partitioning syntax
explain 
analyze table Employee_Part partition (employeeSalary='4000.0', country) compute statistics for columns employeeName, employeeID;
analyze table Employee_Part partition (employeeSalary='4000.0', country) compute statistics for columns employeeName, employeeID;

describe formatted Employee_Part partition (employeeSalary='4000.0', country='USA') employeeName;

-- don't specify all partitioning keys
explain	
analyze table Employee_Part partition (employeeSalary='2000.0') compute statistics for columns employeeID;	
analyze table Employee_Part partition (employeeSalary='2000.0') compute statistics for columns employeeID;

describe formatted Employee_Part partition (employeeSalary='2000.0', country='USA') employeeID;
describe formatted Employee_Part partition (employeeSalary='2000.0', country='UK') employeeID;
-- don't specify any partitioning keys
explain	
analyze table Employee_Part partition (employeeSalary) compute statistics for columns employeeID;	
analyze table Employee_Part partition (employeeSalary) compute statistics for columns employeeID;

describe formatted Employee_Part partition (employeeSalary='3000.0', country='UK') employeeID;
explain	
analyze table Employee_Part partition (employeeSalary,country) compute statistics for columns;	
analyze table Employee_Part partition (employeeSalary,country) compute statistics for columns;

describe formatted Employee_Part partition (employeeSalary='3500.0', country='UK') employeeName;

-- partially populated stats
drop table Employee;
CREATE TABLE Employee(employeeID int, employeeName String) partitioned by (employeeSalary double, country string)
row format delimited fields terminated by '|'  stored as textfile;

LOAD DATA LOCAL INPATH "../../data/files/employee.dat"  INTO TABLE Employee partition(employeeSalary='2000.0', country='USA');
LOAD DATA LOCAL INPATH "../../data/files/employee2.dat" INTO TABLE Employee partition(employeeSalary='2000.0', country='UK');
LOAD DATA LOCAL INPATH "../../data/files/employee2.dat" INTO TABLE Employee partition(employeeSalary='3500.0', country='UK');
LOAD DATA LOCAL INPATH "../../data/files/employee.dat"  INTO TABLE Employee partition(employeeSalary='3000.0', country='UK');

analyze table Employee partition (employeeSalary,country) compute statistics for columns;

describe formatted Employee partition (employeeSalary='3500.0', country='UK') employeeName;
LOAD DATA LOCAL INPATH "../../data/files/employee2.dat" INTO TABLE Employee partition(employeeSalary='3000.0', country='USA');
LOAD DATA LOCAL INPATH "../../data/files/employee2.dat" INTO TABLE Employee partition(employeeSalary='4000.0', country='USA');

analyze table Employee partition (employeeSalary) compute statistics for columns;

describe formatted Employee partition (employeeSalary='3000.0', country='USA') employeeName;

-- add columns
alter table Employee add columns (c int ,d string);

LOAD DATA LOCAL INPATH "../../data/files/employee_part.txt"  INTO TABLE Employee partition(employeeSalary='6000.0', country='UK');

analyze table Employee partition (employeeSalary='6000.0',country='UK') compute statistics for columns;

describe formatted Employee partition (employeeSalary='6000.0', country='UK') employeeName;
describe formatted Employee partition (employeeSalary='6000.0', country='UK') c;
describe formatted Employee partition (employeeSalary='6000.0', country='UK') d;

