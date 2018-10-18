set hive.stats.column.autogather=true;
DROP TABLE Employee_Part;

CREATE TABLE Employee_Part(employeeID int, employeeName String) partitioned by (employeeSalary double, country string)
row format delimited fields terminated by '|'  stored as textfile;

LOAD DATA LOCAL INPATH "../../data/files/employee2.dat" INTO TABLE Employee_Part partition(employeeSalary='2000.0', country='USA');
LOAD DATA LOCAL INPATH "../../data/files/employee2.dat" INTO TABLE Employee_Part partition(employeeSalary='2000.0', country='UK');
LOAD DATA LOCAL INPATH "../../data/files/employee2.dat" INTO TABLE Employee_Part partition(employeeSalary='3000.0', country='USA');
LOAD DATA LOCAL INPATH "../../data/files/employee2.dat" INTO TABLE Employee_Part partition(employeeSalary='4000.0', country='USA');
LOAD DATA LOCAL INPATH "../../data/files/employee2.dat" INTO TABLE Employee_Part partition(employeeSalary='3500.0', country='UK');
LOAD DATA LOCAL INPATH "../../data/files/employee2.dat" INTO TABLE Employee_Part partition(employeeSalary='3000.0', country='UK');

-- specify invalid values for the partitioning keys
explain
analyze table Employee_Part partition (employeeSalary='4000.0', country='Canada') compute statistics for columns employeeName, employeeID;
analyze table Employee_Part partition (employeeSalary='4000.0', country='Canada') compute statistics for columns employeeName, employeeID;
