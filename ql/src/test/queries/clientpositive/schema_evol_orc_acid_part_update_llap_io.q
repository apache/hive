--! qt:dataset:srcbucket
--! qt:dataset:part
--! qt:dataset:alltypesorc
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
set hive.metastore.disallow.incompatible.col.type.changes=true;
set hive.default.fileformat=orc;
set hive.llap.io.enabled=true;
set hive.llap.io.encode.enabled=true;

-- SORT_QUERY_RESULTS
--
-- FILE VARIATION: ORC, ACID Non-Vectorized, MapWork, Partitioned
-- *IMPORTANT NOTE* We set hive.exec.schema.evolution=false above since schema evolution is always used for ACID.
-- Also, we don't do EXPLAINs on ACID files because the write id causes Q file statistics differences...
--

CREATE TABLE schema_evolution_data_n9(insert_num int, boolean1 boolean, tinyint1 tinyint, smallint1 smallint, int1 int, bigint1 bigint, decimal1 decimal(38,18), float1 float, double1 double, string1 string, string2 string, date1 date, timestamp1 timestamp, boolean_str string, tinyint_str string, smallint_str string, int_str string, bigint_str string, decimal_str string, float_str string, double_str string, date_str string, timestamp_str string, filler string)
row format delimited fields terminated by '|' stored as textfile;
load data local inpath '../../data/files/schema_evolution/schema_evolution_data.txt' overwrite into table schema_evolution_data_n9;

CREATE TABLE schema_evolution_data_2_n3(insert_num int, boolean1 boolean, tinyint1 tinyint, smallint1 smallint, int1 int, bigint1 bigint, decimal1 decimal(38,18), float1 float, double1 double, string1 string, string2 string, date1 date, timestamp1 timestamp, boolean_str string, tinyint_str string, smallint_str string, int_str string, bigint_str string, decimal_str string, float_str string, double_str string, date_str string, timestamp_str string, filler string)
row format delimited fields terminated by '|' stored as textfile;
load data local inpath '../../data/files/schema_evolution/schema_evolution_data_2.txt' overwrite into table schema_evolution_data_2_n3;

--
--
-- SECTION VARIATION: ALTER TABLE ADD COLUMNS ... UPDATE New Columns
---
CREATE TABLE partitioned_update_1(insert_num int, a INT, b STRING) PARTITIONED BY(part INT) clustered by (a) into 2 buckets STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into table partitioned_update_1 partition(part=1) SELECT insert_num, int1, 'original' FROM schema_evolution_data_n9;

-- Table-Non-Cascade ADD COLUMNS ...
alter table partitioned_update_1 add columns(c int, d string);

insert into table partitioned_update_1 partition(part=2) SELECT insert_num, int1, 'new', int1, string1 FROM schema_evolution_data_2_n3 WHERE insert_num <=110;

insert into table partitioned_update_1 partition(part=1) SELECT insert_num, int1, 'new', int1, string1 FROM schema_evolution_data_2_n3 WHERE insert_num > 110;

select insert_num,part,a,b,c,d from partitioned_update_1;

-- UPDATE New Columns
update partitioned_update_1 set c=99;

select insert_num,part,a,b,c,d from partitioned_update_1;

alter table partitioned_update_1 partition(part=1) compact 'major';
alter table partitioned_update_1 partition(part=2) compact 'major';

select insert_num,part,a,b,c,d from partitioned_update_1;

DROP TABLE partitioned_update_1;

--
--
-- SECTION VARIATION: ALTER TABLE ADD COLUMNS ... DELETE where old column
---
CREATE TABLE partitioned_delete_1(insert_num int, a INT, b STRING) PARTITIONED BY(part INT) clustered by (a) into 2 buckets STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into table partitioned_delete_1 partition(part=1) SELECT insert_num, int1, 'original' FROM schema_evolution_data_n9;

-- Table-Non-Cascade ADD COLUMNS ...
alter table partitioned_delete_1 add columns(c int, d string);

insert into table partitioned_delete_1 partition(part=2) SELECT insert_num, int1, 'new', int1, string1 FROM schema_evolution_data_2_n3 WHERE insert_num <=110;

insert into table partitioned_delete_1 partition(part=1) SELECT insert_num, int1, 'new', int1, string1 FROM schema_evolution_data_2_n3 WHERE insert_num > 110;

select part,a,b,c,d from partitioned_delete_1;

-- DELETE where old column
delete from partitioned_delete_1 where insert_num = 102 or insert_num = 104 or insert_num = 106;

select insert_num,part,a,b,c,d from partitioned_delete_1;

alter table partitioned_delete_1 partition(part=1) compact 'major';
alter table partitioned_delete_1 partition(part=2) compact 'major';

select insert_num,part,a,b,c,d from partitioned_delete_1;

DROP TABLE partitioned_delete_1;

--
--
-- SECTION VARIATION: ALTER TABLE ADD COLUMNS ... DELETE where new column
---
CREATE TABLE partitioned_delete_2(insert_num int, a INT, b STRING) PARTITIONED BY(part INT) clustered by (a) into 2 buckets STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into table partitioned_delete_2 partition(part=1) SELECT insert_num, int1, 'original' FROM schema_evolution_data_n9;

-- Table-Non-Cascade ADD COLUMNS ...
alter table partitioned_delete_2 add columns(c int, d string);

insert into table partitioned_delete_2 partition(part=2) SELECT insert_num, int1, 'new', int1, string1 FROM schema_evolution_data_2_n3 WHERE insert_num <=110;

insert into table partitioned_delete_2 partition(part=1)  SELECT insert_num, int1, 'new', int1, string1 FROM schema_evolution_data_2_n3 WHERE insert_num > 110;

select insert_num,part,a,b,c,d from partitioned_delete_2;

-- DELETE where new column
delete from partitioned_delete_2 where insert_num = 108 or insert_num > 113;

select insert_num,part,a,b,c,d from partitioned_delete_2;

alter table partitioned_delete_2 partition(part=1) compact 'major';
alter table partitioned_delete_2 partition(part=2) compact 'major';

select insert_num,part,a,b,c,d from partitioned_delete_2;

DROP TABLE partitioned_delete_2;

--following tests is moved from system tests
drop table if exists missing_ddl_2;
create table missing_ddl_2(name string, age int);
insert overwrite table missing_ddl_2 select value, key from srcbucket;
alter table missing_ddl_2 add columns (gps double);

DROP TABLE IF EXISTS all100kjson_textfile_orc;
CREATE TABLE all100kjson_textfile_orc (
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

INSERT INTO TABLE all100kjson_textfile_orc PARTITION (t) SELECT csmallint, cint, cbigint, cfloat, cdouble, cstring1, cboolean1, ctimestamp1, ctinyint FROM alltypesorc WHERE ctinyint > 0;

ALTER TABLE all100kjson_textfile_orc
                            SET FILEFORMAT
                            INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
                            OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
                            SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde';

INSERT INTO TABLE all100kjson_textfile_orc PARTITION (t) SELECT csmallint, cint, cbigint, cfloat, cdouble, cstring1, cboolean1, ctimestamp1, ctinyint FROM alltypesorc WHERE ctinyint < 1 and ctinyint > -50 ;

-- HIVE-11977: Hive should handle an external avro table with zero length files present
DROP TABLE IF EXISTS emptyavro;
CREATE TABLE emptyavro (i int)
               PARTITIONED BY (s string)
               STORED AS AVRO;
load data local inpath '../../data/files/empty1.txt' into table emptyavro PARTITION (s='something');
SELECT COUNT(*) from emptyavro;