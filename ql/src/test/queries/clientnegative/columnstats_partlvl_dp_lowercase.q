set hive.mapred.mode=nonstrict;
DROP TABLE Employee_Part_n0;

CREATE TABLE Employee_Part_n0(employeeID int, employeeName String) partitioned by (employeeSalary double, country string)
row format delimited fields terminated by '|'  stored as textfile;

LOAD DATA LOCAL INPATH "../../data/files/employee.dat"  INTO TABLE Employee_Part_n0 partition(employeeSalary='2000.0', country='USA');
analyze table Employee_Part_n0 partition (employeeSalary='2000.0', country) compute statistics for columns employeeName, employeeID;
describe formatted Employee_Part_n0 partition (employeeSalary='2000.0', country='USA') employeeID;
describe formatted Employee_Part_n0 partition (employeeSalary='2000.0', country='usa') employeeID;