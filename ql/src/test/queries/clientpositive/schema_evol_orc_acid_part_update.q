set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.mapred.mode=nonstrict;
set hive.cli.print.header=true;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.exec.schema.evolution=false;
SET hive.vectorized.use.vectorized.input.format=true;
SET hive.vectorized.use.vector.serde.deserialize=false;
SET hive.vectorized.use.row.serde.deserialize=false;
SET hive.vectorized.execution.enabled=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.metastore.disallow.incompatible.col.type.changes=true;
set hive.default.fileformat=orc;
set hive.llap.io.enabled=false;

-- SORT_QUERY_RESULTS
--
-- FILE VARIATION: ORC, ACID Non-Vectorized, MapWork, Partitioned
-- *IMPORTANT NOTE* We set hive.exec.schema.evolution=false above since schema evolution is always used for ACID.
-- Also, we don't do EXPLAINs on ACID files because the write id causes Q file statistics differences...
--

CREATE TABLE schema_evolution_data_n34(insert_num int, boolean1 boolean, tinyint1 tinyint, smallint1 smallint, int1 int, bigint1 bigint, decimal1 decimal(38,18), float1 float, double1 double, string1 string, string2 string, date1 date, timestamp1 timestamp, boolean_str string, tinyint_str string, smallint_str string, int_str string, bigint_str string, decimal_str string, float_str string, double_str string, date_str string, timestamp_str string, filler string)
row format delimited fields terminated by '|' stored as textfile;
load data local inpath '../../data/files/schema_evolution/schema_evolution_data.txt' overwrite into table schema_evolution_data_n34;

CREATE TABLE schema_evolution_data_2_n10(insert_num int, boolean1 boolean, tinyint1 tinyint, smallint1 smallint, int1 int, bigint1 bigint, decimal1 decimal(38,18), float1 float, double1 double, string1 string, string2 string, date1 date, timestamp1 timestamp, boolean_str string, tinyint_str string, smallint_str string, int_str string, bigint_str string, decimal_str string, float_str string, double_str string, date_str string, timestamp_str string, filler string)
row format delimited fields terminated by '|' stored as textfile;
load data local inpath '../../data/files/schema_evolution/schema_evolution_data_2.txt' overwrite into table schema_evolution_data_2_n10;

--
--
-- SECTION VARIATION: ALTER TABLE ADD COLUMNS ... UPDATE New Columns
---
CREATE TABLE partitioned_update_1_n1(insert_num int, a INT, b STRING) PARTITIONED BY(part INT) clustered by (a) into 2 buckets STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into table partitioned_update_1_n1 partition(part=1) SELECT insert_num, int1, 'original' FROM schema_evolution_data_n34;

-- Table-Non-Cascade ADD COLUMNS ...
alter table partitioned_update_1_n1 add columns(c int, d string);

insert into table partitioned_update_1_n1 partition(part=2) SELECT insert_num, int1, 'new', int1, string1 FROM schema_evolution_data_2_n10 WHERE insert_num <=110;

insert into table partitioned_update_1_n1 partition(part=1) SELECT insert_num, int1, 'new', int1, string1 FROM schema_evolution_data_2_n10 WHERE insert_num > 110;

select insert_num,part,a,b,c,d from partitioned_update_1_n1;

-- UPDATE New Columns
update partitioned_update_1_n1 set c=99;

select insert_num,part,a,b,c,d from partitioned_update_1_n1;

alter table partitioned_update_1_n1 partition(part=1) compact 'major';
alter table partitioned_update_1_n1 partition(part=2) compact 'major';

select insert_num,part,a,b,c,d from partitioned_update_1_n1;

DROP TABLE partitioned_update_1_n1;

--
--
-- SECTION VARIATION: ALTER TABLE ADD COLUMNS ... DELETE where old column
---
CREATE TABLE partitioned_delete_1_n1(insert_num int, a INT, b STRING) PARTITIONED BY(part INT) clustered by (a) into 2 buckets STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into table partitioned_delete_1_n1 partition(part=1) SELECT insert_num, int1, 'original' FROM schema_evolution_data_n34;

-- Table-Non-Cascade ADD COLUMNS ...
alter table partitioned_delete_1_n1 add columns(c int, d string);

insert into table partitioned_delete_1_n1 partition(part=2) SELECT insert_num, int1, 'new', int1, string1 FROM schema_evolution_data_2_n10 WHERE insert_num <=110;

insert into table partitioned_delete_1_n1 partition(part=1) SELECT insert_num, int1, 'new', int1, string1 FROM schema_evolution_data_2_n10 WHERE insert_num > 110;

select part,a,b,c,d from partitioned_delete_1_n1;

-- DELETE where old column
delete from partitioned_delete_1_n1 where insert_num = 102 or insert_num = 104 or insert_num = 106;

select insert_num,part,a,b,c,d from partitioned_delete_1_n1;

alter table partitioned_delete_1_n1 partition(part=1) compact 'major';
alter table partitioned_delete_1_n1 partition(part=2) compact 'major';

select insert_num,part,a,b,c,d from partitioned_delete_1_n1;

DROP TABLE partitioned_delete_1_n1;

--
--
-- SECTION VARIATION: ALTER TABLE ADD COLUMNS ... DELETE where new column
---
CREATE TABLE partitioned_delete_2_n1(insert_num int, a INT, b STRING) PARTITIONED BY(part INT) clustered by (a) into 2 buckets STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into table partitioned_delete_2_n1 partition(part=1) SELECT insert_num, int1, 'original' FROM schema_evolution_data_n34;

-- Table-Non-Cascade ADD COLUMNS ...
alter table partitioned_delete_2_n1 add columns(c int, d string);

insert into table partitioned_delete_2_n1 partition(part=2) SELECT insert_num, int1, 'new', int1, string1 FROM schema_evolution_data_2_n10 WHERE insert_num <=110;

insert into table partitioned_delete_2_n1 partition(part=1)  SELECT insert_num, int1, 'new', int1, string1 FROM schema_evolution_data_2_n10 WHERE insert_num > 110;

select insert_num,part,a,b,c,d from partitioned_delete_2_n1;

-- DELETE where new column
delete from partitioned_delete_2_n1 where insert_num = 108 or insert_num > 113;

select insert_num,part,a,b,c,d from partitioned_delete_2_n1;

alter table partitioned_delete_2_n1 partition(part=1) compact 'major';
alter table partitioned_delete_2_n1 partition(part=2) compact 'major';

select insert_num,part,a,b,c,d from partitioned_delete_2_n1;

DROP TABLE partitioned_delete_2_n1;

--following tests is moved from system tests
drop table if exists missing_ddl_2_n0;
create table missing_ddl_2_n0(name string, age int);
insert overwrite table missing_ddl_2_n0 select value, key from srcbucket;
alter table missing_ddl_2_n0 add columns (gps double);

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=true;

DROP TABLE IF EXISTS all100kjson_textfile_orc_n0;
CREATE TABLE all100kjson_textfile_orc_n0 (
                             si smallint,
                             i int,
                             b bigint,
                             f float,
                             d double,
                             s string,
                             bo boolean,
                             ts timestamp)
                             PARTITIONED BY (t tinyint)
                             ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
                             WITH SERDEPROPERTIES ('timestamp.formats'='yyyy-MM-dd\'T\'HH:mm:ss')
                             STORED AS TEXTFILE;

INSERT INTO TABLE all100kjson_textfile_orc_n0 PARTITION (t) SELECT csmallint, cint, cbigint, cfloat, cdouble, cstring1, cboolean1, ctimestamp1, ctinyint FROM alltypesorc WHERE ctinyint > 0;

ALTER TABLE all100kjson_textfile_orc_n0
                            SET FILEFORMAT
                            INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
                            OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
                            SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde';

INSERT INTO TABLE all100kjson_textfile_orc_n0 PARTITION (t) SELECT csmallint, cint, cbigint, cfloat, cdouble, cstring1, cboolean1, ctimestamp1, ctinyint FROM alltypesorc WHERE ctinyint < 1 and ctinyint > -50 ;

-- HIVE-11977: Hive should handle an external avro table with zero length files present
DROP TABLE IF EXISTS emptyavro_n1;
CREATE TABLE emptyavro_n1 (i int)
               PARTITIONED BY (s string)
               STORED AS AVRO;
load data local inpath '../../data/files/empty1.txt' into table emptyavro_n1 PARTITION (s='something');
SELECT COUNT(*) from emptyavro_n1;