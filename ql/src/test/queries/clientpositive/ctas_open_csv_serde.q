create table T1_char(abc char(1));
insert into table T1_char values('a'), ('b'), ('c'), ('d');
create table T2_char row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' with serdeproperties
("separatorChar" = ',' , "quoteChar" = '"') stored as textfile as select * from T1_char;
select * from T2_char;

create table T1_varchar(abc varchar (10));
insert into table T1_varchar values('abc'), ('bca'), ('cde'), ('defg');
create table T2_varchar row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' with serdeproperties
("separatorChar" = ',' , "quoteChar" = '"') stored as textfile as select * from T1_varchar;
select * from T2_varchar;

create table T1_integer(abc int);
insert into table T1_integer values(1), (2), (3), (4);
create table T2_integer row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' with serdeproperties
("separatorChar" = ',' , "quoteChar" = '"') stored as textfile as select * from T1_integer;
select * from T2_integer;

create table T1_tinyInt(abc tinyint);
insert into table T1_tinyInt values(11), (21), (32), (42);
create table T2_tinyInt row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' with serdeproperties
("separatorChar" = ',' , "quoteChar" = '"') stored as textfile as select * from T1_tinyInt;
select * from T2_tinyInt;

create table T1_bigInt(abc bigint);
insert into table T1_bigInt values(11), (21), (32), (42);
create table T2_bigInt row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' with serdeproperties
("separatorChar" = ',' , "quoteChar" = '"') stored as textfile as select * from T1_bigInt;
select * from T2_bigInt;

create table T1_decimal(abc decimal(10,2));
insert into table T1_decimal values(1.25), (3.332), (6.2), (10);
create table T2_decimal row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' with serdeproperties
("separatorChar" = ',' , "quoteChar" = '"') stored as textfile as select * from T1_decimal;
select * from T2_decimal;

create table T1_float(abc float);
insert into table T1_float values(2.25), (4.33), (6.99), (9.88);
create table T2_float row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' with serdeproperties
("separatorChar" = ',' , "quoteChar" = '"') stored as textfile as select * from T1_float;
select * from T2_float;

create table T1_boolean(abc boolean);
insert into table T1_boolean values(true), (false), (true), (true);
create table T2_boolean row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' with serdeproperties
("separatorChar" = ',' , "quoteChar" = '"') stored as textfile as select * from T1_boolean;
select * from T2_boolean;

create table T1_date(abc date);
insert into table T1_date values(cast('2014-01-01' as date)), (cast('2016-08-07' as date)),
(cast('2018-03-30' as date)), (cast('2019-02-14' as date));
create table T2_date row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' with serdeproperties
("separatorChar" = ',' , "quoteChar" = '"') stored as textfile as select * from T1_date;
select * from T2_date;

create table T1_timestamp(abc timestamp);
insert into table T1_timestamp values (cast('2016-01-01 00:00:00' as timestamp));
create table T2_timestamp row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' with serdeproperties
("separatorChar" = ',' , "quoteChar" = '"') stored as textfile as select * from T1_timestamp;
select * from T2_timestamp;


-- HIVE-30205, execute CTAS with and without vectorization
-- The two tables T2_vec and T2_novec are expected to have the same content
set hive.vectorized.execution.enabled=true;

create table T1_vec(f1 char(1), f2 varchar(10), f3 int, f4 tinyint, f5 bigint, f6 decimal(10,2), f7 float, f8 boolean, f9 date, f10 timestamp, f11 binary);
insert into T1_vec values ('a', 'abc', 1, 11, 11, 1.25, 2.25, true, cast('2024-01-01' as date), cast('2016-01-01 00:00:00' as timestamp), encode('abc', 'UTF-8'));
create table T2_vec row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' with serdeproperties
("separatorChar" = ',' , "quoteChar" = '"') stored as textfile as select * from T1_vec;
select * from T2_vec;

set hive.vectorized.execution.enabled=false;

create table T1_novec(f1 char(1), f2 varchar(10), f3 int, f4 tinyint, f5 bigint, f6 decimal(10,2), f7 float, f8 boolean, f9 date, f10 timestamp, f11 binary);
insert into T1_novec values ('a', 'abc', 1, 11, 11, 1.25, 2.25, true, cast('2024-01-01' as date), cast('2016-01-01 00:00:00' as timestamp), encode('abc', 'UTF-8'));
create table T2_novec row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' with serdeproperties
("separatorChar" = ',' , "quoteChar" = '"') stored as textfile as select * from T1_novec;
select * from T2_novec;

