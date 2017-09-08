set hive.stats.autogather=false;

create table supplier_nostats (S_SUPPKEY INT, S_NAME STRING, S_ADDRESS STRING, S_NATIONKEY INT,
S_PHONE STRING, S_ACCTBAL DOUBLE, S_COMMENT STRING);

CREATE TABLE lineitem_nostats (L_ORDERKEY      INT,
                                L_PARTKEY       INT,
                                L_SUPPKEY       INT,
                                L_LINENUMBER    INT,
                                L_QUANTITY      DOUBLE,
                                L_EXTENDEDPRICE DOUBLE,
                                L_DISCOUNT      DOUBLE,
                                L_TAX           DOUBLE,
                                L_RETURNFLAG    STRING,
                                L_LINESTATUS    STRING,
                                l_shipdate      STRING,
                                L_COMMITDATE    STRING,
                                L_RECEIPTDATE   STRING,
                                L_SHIPINSTRUCT  STRING,
                                L_SHIPMODE      STRING,
                                L_COMMENT       STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

CREATE TABLE part_nostats(
    p_partkey INT,
    p_name STRING,
    p_mfgr STRING,
    p_brand STRING,
    p_type STRING,
    p_size INT,
    p_container STRING,
    p_retailprice DOUBLE,
    p_comment STRING
);

-- should not have cross join
explain select count(1) from part_nostats,supplier_nostats,lineitem_nostats where p_partkey = l_partkey and s_suppkey = l_suppkey;

set hive.stats.estimate=false;
explain select count(1) from part_nostats,supplier_nostats,lineitem_nostats where p_partkey = l_partkey and s_suppkey = l_suppkey;

CREATE TABLE Employee_Part(employeeID int, employeeName String) partitioned by (employeeSalary double, country string)
row format delimited fields terminated by '|'  stored as textfile;

LOAD DATA LOCAL INPATH "../../data/files/employee.dat"  INTO TABLE Employee_Part partition(employeeSalary='2000.0', country='USA');
LOAD DATA LOCAL INPATH "../../data/files/employee2.dat" INTO TABLE Employee_Part partition(employeeSalary='2000.0', country='UK');
LOAD DATA LOCAL INPATH "../../data/files/employee2.dat" INTO TABLE Employee_Part partition(employeeSalary='3000.0', country='USA');
LOAD DATA LOCAL INPATH "../../data/files/employee2.dat" INTO TABLE Employee_Part partition(employeeSalary='4000.0', country='USA');
LOAD DATA LOCAL INPATH "../../data/files/employee2.dat" INTO TABLE Employee_Part partition(employeeSalary='3500.0', country='UK');
LOAD DATA LOCAL INPATH "../../data/files/employee.dat"  INTO TABLE Employee_Part partition(employeeSalary='3000.0', country='UK');

-- partitioned table
set hive.stats.estimate=true;
explain select count(1) from Employee_Part,supplier_nostats,lineitem_nostats where employeeID= l_partkey and s_suppkey = l_suppkey;

set hive.stats.estimate=false;
explain select count(1) from Employee_Part,supplier_nostats,lineitem_nostats where employeeID= l_partkey and s_suppkey = l_suppkey;

drop table Employee_Part;
drop table supplier_nostats;
drop table lineitem_nostats;
drop table part_nostats;
