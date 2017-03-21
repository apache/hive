set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.mapred.mode=nonstrict;
set hive.cli.print.header=true;
SET hive.exec.schema.evolution=true;
SET hive.vectorized.use.vectorized.input.format=true;
SET hive.vectorized.use.vector.serde.deserialize=false;
SET hive.vectorized.use.row.serde.deserialize=false;
SET hive.vectorized.execution.enabled=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.metastore.disallow.incompatible.col.type.changes=false;
set hive.default.fileformat=orc;
set hive.llap.io.enabled=false;

-- SORT_QUERY_RESULTS
--
-- FILE VARIATION: ORC, Vectorized, MapWork, Partitioned --> all complex conversions
--
------------------------------------------------------------------------------------------
--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for Various --> Various: STRUCT<BOOLEAN, TINYINT, SMALLINT, INT, LONG, FLOAT, DOUBLE, DECIMAL(38,18), CHAR, VARCHAR, TIMESTAMP, DATE, BINARY> --> STRUCT<STRING...
--
CREATE TABLE part_change_various_various_struct1(insert_num int, s1 STRUCT<c1:BOOLEAN, c2:TINYINT, c3:SMALLINT, c4:INT, c5:BIGINT, c6:FLOAT, c7:DOUBLE, c8:DECIMAL(38,18), c9:CHAR(25), c10:VARCHAR(25), c11:TIMESTAMP, c12:DATE, c13:BINARY>, b STRING) PARTITIONED BY(part INT);

CREATE TABLE complex_struct1_a_txt(insert_num int, s1 STRUCT<c1:BOOLEAN, c2:TINYINT, c3:SMALLINT, c4:INT, c5:BIGINT, c6:FLOAT, c7:DOUBLE, c8:DECIMAL(38,18), c9:CHAR(25), c10:VARCHAR(25), c11:TIMESTAMP, c12:DATE, c13:BINARY>, b STRING)
row format delimited fields terminated by '|'
collection items terminated by ','
map keys terminated by ':' stored as textfile;
load data local inpath '../../data/files/schema_evolution/complex_struct1_a.txt' overwrite into table complex_struct1_a_txt;

insert into table part_change_various_various_struct1 partition(part=1) select * from complex_struct1_a_txt;

select insert_num,part,s1,b from part_change_various_various_struct1;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table part_change_various_various_struct1 replace columns (insert_num int, s1 STRUCT<c1:STRING, c2:STRING, c3:STRING, c4:STRING, c5:STRING, c6:STRING, c7:STRING, c8:STRING, c9:STRING, c10:STRING, c11:STRING, c12:STRING, c13:STRING>, b STRING);

CREATE TABLE complex_struct1_b_txt(insert_num int, s1 STRUCT<c1:STRING, c2:STRING, c3:STRING, c4:STRING, c5:STRING, c6:STRING, c7:STRING, c8:STRING, c9:STRING, c10:STRING, c11:STRING, c12:STRING, c13:STRING>, b STRING)
row format delimited fields terminated by '|'
collection items terminated by ','
map keys terminated by ':' stored as textfile;
load data local inpath '../../data/files/schema_evolution/complex_struct1_b.txt' overwrite into table complex_struct1_b_txt;

insert into table part_change_various_various_struct1 partition(part=2) select * from complex_struct1_b_txt;

CREATE TABLE complex_struct1_c_txt(insert_num int, s1 STRUCT<c1:STRING, c2:STRING, c3:STRING, c4:STRING, c5:STRING, c6:STRING, c7:STRING, c8:STRING, c9:STRING, c10:STRING, c11:STRING, c12:STRING, c13:STRING>, b STRING)
row format delimited fields terminated by '|'
collection items terminated by ','
map keys terminated by ':' stored as textfile;
load data local inpath '../../data/files/schema_evolution/complex_struct1_c.txt' overwrite into table complex_struct1_c_txt;

insert into table part_change_various_various_struct1 partition(part=1) select * from complex_struct1_c_txt;

 explain vectorization detail
select insert_num,part,s1,b from part_change_various_various_struct1;

select insert_num,part,s1,b from part_change_various_various_struct1;

drop table part_change_various_various_struct1;



--
-- SUBSECTION: ALTER TABLE ADD COLUMNS for Various --> Various: STRUCT
--
CREATE TABLE part_add_various_various_struct2(insert_num int, b STRING) PARTITIONED BY(part INT);

insert into table part_add_various_various_struct2 partition(part=1)
    values(1, 'original'),
          (2, 'original');

select insert_num,part,b from part_add_various_various_struct2;

-- Table-Non-Cascade ADD COLUMN ...
alter table part_add_various_various_struct2 ADD columns (s2 STRUCT<c1:BOOLEAN, c2:TINYINT, c3:SMALLINT, c4:INT, c5:BIGINT, c6:FLOAT, c7:DOUBLE, c8:DECIMAL(38,18), c9:CHAR(25), c10:VARCHAR(25), c11:TIMESTAMP, c12:DATE, c13:BINARY>);

CREATE TABLE complex_struct2_a_txt(insert_num int, b STRING, s2 STRUCT<c1:BOOLEAN, c2:TINYINT, c3:SMALLINT, c4:INT, c5:BIGINT, c6:FLOAT, c7:DOUBLE, c8:DECIMAL(38,18), c9:CHAR(25), c10:VARCHAR(25), c11:TIMESTAMP, c12:DATE, c13:BINARY>)
row format delimited fields terminated by '|'
collection items terminated by ','
map keys terminated by ':' stored as textfile;
load data local inpath '../../data/files/schema_evolution/complex_struct2_a.txt' overwrite into table complex_struct2_a_txt;

insert into table part_add_various_various_struct2 partition(part=1) select * from complex_struct2_a_txt;

CREATE TABLE complex_struct2_b_txt(insert_num int, b STRING, s2 STRUCT<c1:BOOLEAN, c2:TINYINT, c3:SMALLINT, c4:INT, c5:BIGINT, c6:FLOAT, c7:DOUBLE, c8:DECIMAL(38,18), c9:CHAR(25), c10:VARCHAR(25), c11:TIMESTAMP, c12:DATE, c13:BINARY>)
row format delimited fields terminated by '|'
collection items terminated by ','
map keys terminated by ':' stored as textfile;
load data local inpath '../../data/files/schema_evolution/complex_struct2_b.txt' overwrite into table complex_struct2_b_txt;

insert into table part_add_various_various_struct2 partition(part=2) select * from complex_struct2_b_txt;

select insert_num,part,b,s2 from part_add_various_various_struct2;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table part_add_various_various_struct2 REPLACE columns (insert_num int, b STRING, s2 STRUCT<c1:STRING, c2:STRING, c3:STRING, c4:STRING, c5:STRING, c6:STRING, c7:STRING, c8:STRING, c9:STRING, c10:STRING, c11:STRING, c12:STRING, c13:STRING>);

CREATE TABLE complex_struct2_c_txt(insert_num int, b STRING, s2 STRUCT<c1:STRING, c2:STRING, c3:STRING, c4:STRING, c5:STRING, c6:STRING, c7:STRING, c8:STRING, c9:STRING, c10:STRING, c11:STRING, c12:STRING, c13:STRING>)
row format delimited fields terminated by '|'
collection items terminated by ','
map keys terminated by ':' stored as textfile;
load data local inpath '../../data/files/schema_evolution/complex_struct2_c.txt' overwrite into table complex_struct2_c_txt;

insert into table part_add_various_various_struct2 partition(part=2) select * from complex_struct2_c_txt;

CREATE TABLE complex_struct2_d_txt(insert_num int, b STRING, s2 STRUCT<c1:STRING, c2:STRING, c3:STRING, c4:STRING, c5:STRING, c6:STRING, c7:STRING, c8:STRING, c9:STRING, c10:STRING, c11:STRING, c12:STRING, c13:STRING>)
row format delimited fields terminated by '|'
collection items terminated by ','
map keys terminated by ':' stored as textfile;
load data local inpath '../../data/files/schema_evolution/complex_struct2_d.txt' overwrite into table complex_struct2_d_txt;

insert into table part_add_various_various_struct2 partition(part=1) select * from complex_struct2_d_txt;

explain vectorization detail
select insert_num,part,b,s2 from part_add_various_various_struct2;

select insert_num,part,b,s2 from part_add_various_various_struct2;

drop table part_add_various_various_struct2;




--
-- SUBSECTION: ALTER TABLE ADD COLUMNS for Various --> Various: ADD COLUMNS to STRUCT type as LAST column of 3 columns
--
CREATE TABLE part_add_to_various_various_struct4(insert_num int, b STRING, s3 STRUCT<c1:BOOLEAN, c2:TINYINT, c3:SMALLINT, c4:INT, c5:BIGINT>) PARTITIONED BY(part INT);

CREATE TABLE complex_struct4_a_txt(insert_num int, b STRING, s3 STRUCT<c1:BOOLEAN, c2:TINYINT, c3:SMALLINT, c4:INT, c5:BIGINT>)
row format delimited fields terminated by '|'
collection items terminated by ','
map keys terminated by ':' stored as textfile;
load data local inpath '../../data/files/schema_evolution/complex_struct4_a.txt' overwrite into table complex_struct4_a_txt;

insert into table part_add_to_various_various_struct4 partition(part=1) select * from complex_struct4_a_txt;

select insert_num,part,b,s3 from part_add_to_various_various_struct4;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table part_add_to_various_various_struct4 replace columns (insert_num int, b STRING, s3 STRUCT<c1:BOOLEAN, c2:TINYINT, c3:SMALLINT, c4:INT, c5:BIGINT, c6:FLOAT, c7:DOUBLE, c8:DECIMAL(38,18), c9:CHAR(25), c10:VARCHAR(25), c11:TIMESTAMP, c12:DATE, c13:BINARY>);

CREATE TABLE complex_struct4_b_txt(insert_num int, b STRING, s3 STRUCT<c1:BOOLEAN, c2:TINYINT, c3:SMALLINT, c4:INT, c5:BIGINT, c6:FLOAT, c7:DOUBLE, c8:DECIMAL(38,18), c9:CHAR(25), c10:VARCHAR(25), c11:TIMESTAMP, c12:DATE, c13:BINARY>)
row format delimited fields terminated by '|'
collection items terminated by ','
map keys terminated by ':' stored as textfile;
load data local inpath '../../data/files/schema_evolution/complex_struct4_b.txt' overwrite into table complex_struct4_b_txt;

insert into table part_add_to_various_various_struct4 partition(part=2) select * from complex_struct4_b_txt;

CREATE TABLE complex_struct4_c_txt(insert_num int, b STRING, s3 STRUCT<c1:BOOLEAN, c2:TINYINT, c3:SMALLINT, c4:INT, c5:BIGINT, c6:FLOAT, c7:DOUBLE, c8:DECIMAL(38,18), c9:CHAR(25), c10:VARCHAR(25), c11:TIMESTAMP, c12:DATE, c13:BINARY>)
row format delimited fields terminated by '|'
collection items terminated by ','
map keys terminated by ':' stored as textfile;
load data local inpath '../../data/files/schema_evolution/complex_struct4_c.txt' overwrite into table complex_struct4_c_txt;

insert into table part_add_to_various_various_struct4 partition(part=1) select * from complex_struct4_c_txt;

explain vectorization detail
select insert_num,part,b,s3 from part_add_to_various_various_struct4;

select insert_num,part,b,s3 from part_add_to_various_various_struct4;

drop table part_add_to_various_various_struct4;
