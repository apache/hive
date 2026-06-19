set hive.vectorized.execution.enabled=true;

CREATE EXTERNAL TABLE ice_date   (`col1` int, `day` date, `calday` date) PARTITIONED BY SPEC (calday)   stored by
iceberg tblproperties('format-version'='2');
insert into ice_date values(1, '2020-11-20', '2020-11-20'), (1, '2020-11-20', '2020-11-20');
select * from ice_date;
select count(calday) from ice_date;
select distinct(calday) from ice_date;


CREATE EXTERNAL TABLE ice_timestamp   (`col1` int, `day` date, `times` timestamp) PARTITIONED BY SPEC (times)   stored
by iceberg tblproperties('format-version'='2');
insert into ice_timestamp values(1, '2020-11-20', '2020-11-20'), (1, '2020-11-20', '2020-11-20');
select * from ice_timestamp;
select count(times) from ice_timestamp;
select distinct(times) from ice_timestamp;


CREATE EXTERNAL TABLE ice_decimal  (`col1` int, `decimalA` decimal(5,2), `decimalC` decimal(5,2)) PARTITIONED BY SPEC
(decimalC) stored by iceberg tblproperties('format-version'='2');
insert into ice_decimal values(1, 122.91, 102.21), (1, 12.32, 200.12);
select * from ice_decimal;
select distinct(decimalc) from ice_decimal;
select count(decimala) from ice_decimal where decimala=122.91;
