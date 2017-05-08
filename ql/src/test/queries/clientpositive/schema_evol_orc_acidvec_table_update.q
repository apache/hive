set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.cli.print.header=true;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.exec.schema.evolution=false;
SET hive.vectorized.use.vectorized.input.format=true;
SET hive.vectorized.use.vector.serde.deserialize=false;
SET hive.vectorized.use.row.serde.deserialize=false;
SET hive.vectorized.execution.enabled=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.metastore.disallow.incompatible.col.type.changes=true;
set hive.default.fileformat=orc;
set hive.llap.io.enabled=false;

-- SORT_QUERY_RESULTS
--
-- FILE VARIATION: ORC, ACID Vectorized, MapWork, Table
-- *IMPORTANT NOTE* We set hive.exec.schema.evolution=false above since schema evolution is always used for ACID.
-- Also, we don't do EXPLAINs on ACID files because the transaction id causes Q file statistics differences...
--

CREATE TABLE schema_evolution_data(insert_num int, boolean1 boolean, tinyint1 tinyint, smallint1 smallint, int1 int, bigint1 bigint, decimal1 decimal(38,18), float1 float, double1 double, string1 string, string2 string, date1 date, timestamp1 timestamp, boolean_str string, tinyint_str string, smallint_str string, int_str string, bigint_str string, decimal_str string, float_str string, double_str string, date_str string, timestamp_str string, filler string)
row format delimited fields terminated by '|' stored as textfile;
load data local inpath '../../data/files/schema_evolution/schema_evolution_data.txt' overwrite into table schema_evolution_data;

CREATE TABLE schema_evolution_data_2(insert_num int, boolean1 boolean, tinyint1 tinyint, smallint1 smallint, int1 int, bigint1 bigint, decimal1 decimal(38,18), float1 float, double1 double, string1 string, string2 string, date1 date, timestamp1 timestamp, boolean_str string, tinyint_str string, smallint_str string, int_str string, bigint_str string, decimal_str string, float_str string, double_str string, date_str string, timestamp_str string, filler string)
row format delimited fields terminated by '|' stored as textfile;
load data local inpath '../../data/files/schema_evolution/schema_evolution_data_2.txt' overwrite into table schema_evolution_data_2;

--
--
-- SECTION VARIATION: ALTER TABLE ADD COLUMNS ... UPDATE New Columns
---
CREATE TABLE table5(insert_num int, a INT, b STRING) clustered by (a) into 2 buckets STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into table table5 SELECT insert_num, int1, 'original' FROM schema_evolution_data;

-- Table-Non-Cascade ADD COLUMNS ...
alter table table5 add columns(c int, d string);

insert into table table5 SELECT insert_num, int1, 'new', int1, string1 FROM schema_evolution_data_2;

select a,b,c,d from table5;

-- UPDATE New Columns
update table5 set c=99;

select a,b,c,d from table5;

alter table table5 compact 'major';

select a,b,c,d from table5;

DROP TABLE table5;

--
--
-- SECTION VARIATION: ALTER TABLE ADD COLUMNS ... DELETE where old column
---
CREATE TABLE table6(insert_num int, a INT, b STRING) clustered by (a) into 2 buckets STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into table table6 SELECT insert_num, int1, 'original' FROM schema_evolution_data;

-- Table-Non-Cascade ADD COLUMNS ...
alter table table6 add columns(c int, d string);

insert into table table6 SELECT insert_num, int1, 'new', int1, string1 FROM schema_evolution_data_2 WHERE insert_num <= 110;

insert into table table6 SELECT insert_num, int1, 'new', int1, string1 FROM schema_evolution_data_2 WHERE insert_num > 110;

select a,b,c,d from table6;

-- DELETE where old column
delete from table6 where insert_num = 102 or insert_num = 104 or insert_num = 106;

select a,b,c,d from table6;

alter table table6 compact 'major';

select a,b,c,d from table6;

DROP TABLE table6;

--
--
-- SECTION VARIATION: ALTER TABLE ADD COLUMNS ... DELETE where new column
---
CREATE TABLE table7(insert_num int, a INT, b STRING) clustered by (a) into 2 buckets STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into table table7 SELECT insert_num, int1, 'original' FROM schema_evolution_data;

-- Table-Non-Cascade ADD COLUMNS ...
alter table table7 add columns(c int, d string);

insert into table table7 SELECT insert_num, int1, 'new', int1, string1 FROM schema_evolution_data_2 WHERE insert_num <= 110;

insert into table table7 SELECT insert_num, int1, 'new', int1, string1 FROM schema_evolution_data_2 WHERE insert_num > 110;

select a,b,c,d from table7;

-- DELETE where new column
delete from table7 where insert_num = 107 or insert_num >= 110;

select a,b,c,d from table7;

alter table table7 compact 'major';

select a,b,c,d from table7;

DROP TABLE table7;
