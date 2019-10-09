--! qt:dataset:part
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.mapred.mode=nonstrict;
set hive.cli.print.header=true;
SET hive.exec.schema.evolution=true;
SET hive.vectorized.use.vectorized.input.format=false;
SET hive.vectorized.use.vector.serde.deserialize=false;
SET hive.vectorized.use.row.serde.deserialize=true;
SET hive.vectorized.execution.enabled=true;
set hive.metastore.disallow.incompatible.col.type.changes=false;
set hive.default.fileformat=textfile;
set hive.llap.io.enabled=true;set hive.llap.io.encode.enabled=true;

-- SORT_QUERY_RESULTS
--
-- FILE VARIATION: TEXTFILE, Non-Vectorized, MapWork, Partitioned --> all primitive conversions
-- NOTE: the use of hive.vectorized.use.row.serde.deserialize above which enables doing
--  vectorized reading of TEXTFILE format files using the row SERDE methods.
--
------------------------------------------------------------------------------------------
-- SECTION: ALTER TABLE CHANGE COLUMNS Various --> Various
--
--

CREATE TABLE schema_evolution_data_n2(insert_num int, boolean1 boolean, tinyint1 tinyint, smallint1 smallint, int1 int, bigint1 bigint, decimal1 decimal(38,18), float1 float, double1 double, string1 string, string2 string, date1 date, timestamp1 timestamp, boolean_str string, tinyint_str string, smallint_str string, int_str string, bigint_str string, decimal_str string, float_str string, double_str string, date_str string, timestamp_str string, filler string)
row format delimited fields terminated by '|' stored as textfile;
load data local inpath '../../data/files/schema_evolution/schema_evolution_data.txt' overwrite into table schema_evolution_data_n2;

CREATE TABLE schema_evolution_data_2_n0(insert_num int, boolean1 boolean, tinyint1 tinyint, smallint1 smallint, int1 int, bigint1 bigint, decimal1 decimal(38,18), float1 float, double1 double, string1 string, string2 string, date1 date, timestamp1 timestamp, boolean_str string, tinyint_str string, smallint_str string, int_str string, bigint_str string, decimal_str string, float_str string, double_str string, date_str string, timestamp_str string, filler string)
row format delimited fields terminated by '|' stored as textfile;
load data local inpath '../../data/files/schema_evolution/schema_evolution_data_2.txt' overwrite into table schema_evolution_data_2_n0;

--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for Various --> Various:
--            (BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DECIMAL, STRING, TIMESTAMP) --> BOOLEAN and
--            (BOOLEAN, SHORT, INT, LONG, FLOAT, DOUBLE, DECIMAL, STRING, CHAR, VARCHAR, TIMESTAMP) --> BYTE   128 and a maximum value of 127 and
--            (BOOLEAN, TINYINT, INT, LONG, FLOAT, DOUBLE, DECIMAL, STRING, CHAR, VARCHAR, TIMESTAMP) --> SMALLINT   -32768 and a maximum value of 32767 and
--            (BOOLEAN, TINYINT, SMALLINT, LONG, FLOAT, DOUBLE, DECIMAL, STRING, CHAR, VARCHAR, TIMESTAMP) --> INT    –2147483648 to 2147483647 and
--            (BOOLEAN, TINYINT, SMALLINT, INT, FLOAT, DOUBLE, DECIMAL, STRING, CHAR, VARCHAR, TIMESTAMP) --> BIGINT   -9223372036854775808 to 9223372036854775807
--
CREATE TABLE part_change_various_various_boolean_to_bigint(insert_num int,
              c1 TINYINT, c2 SMALLINT, c3 INT, c4 BIGINT, c5 FLOAT, c6 DOUBLE, c7 DECIMAL(38,18), c8 STRING, c9 TIMESTAMP,
              c10 BOOLEAN, c11 SMALLINT, c12 INT, c13 BIGINT, c14 FLOAT, c15 DOUBLE, c16 DECIMAL(38,18), c17 STRING, c18 CHAR(25), c19 VARCHAR(25), c20 TIMESTAMP,
              c21 BOOLEAN, c22 TINYINT, c23 INT, c24 BIGINT, c25 FLOAT, c26 DOUBLE, c27 DECIMAL(38,18), c28 STRING, c29 CHAR(25), c30 VARCHAR(25), c31 TIMESTAMP,
              c32 BOOLEAN, c33 TINYINT, c34 SMALLINT, c35 BIGINT, c36 FLOAT, c37 DOUBLE, c38 DECIMAL(38,18), c39 STRING, c40 CHAR(25), c41 VARCHAR(25), c42 TIMESTAMP,
              c43 BOOLEAN, c44 TINYINT, c45 SMALLINT, c46 INT, c47 FLOAT, c48 DOUBLE, c49 DECIMAL(38,18), c50 STRING, c51 CHAR(25), c52 VARCHAR(25), c53 TIMESTAMP,
              b STRING) PARTITIONED BY(part INT);

insert into table part_change_various_various_boolean_to_bigint partition(part=1) SELECT insert_num,
             tinyint1, smallint1, int1, bigint1, float1, double1, decimal1, boolean_str, timestamp1,
             boolean1, smallint1, int1, bigint1, float1, double1, decimal1, tinyint_str, tinyint_str, tinyint_str, timestamp1,
             boolean1, tinyint1, int1, bigint1, float1, double1, decimal1, smallint_str, smallint_str, smallint_str, timestamp1,
             boolean1, tinyint1, smallint1, bigint1, float1, double1, decimal1, int_str, int_str, int_str, timestamp1,
             boolean1, tinyint1, smallint1, int1, float1, double1, decimal1, bigint_str, bigint_str, bigint_str, timestamp1,
             'original' FROM schema_evolution_data_n2;

explain vectorization detail
select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,c30,c31,c32,c33,c34,c35,c36,c37,c38,c39,c40,c41,c42,c43,c44,c45,c46,c47,c48,c49,c50,c51,c52,c53,b from part_change_various_various_boolean_to_bigint;

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,c30,c31,c32,c33,c34,c35,c36,c37,c38,c39,c40,c41,c42,c43,c44,c45,c46,c47,c48,c49,c50,c51,c52,c53,b from part_change_various_various_boolean_to_bigint;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table part_change_various_various_boolean_to_bigint replace columns (insert_num int,
             c1 BOOLEAN, c2 BOOLEAN, c3 BOOLEAN, c4 BOOLEAN, c5 BOOLEAN, c6 BOOLEAN, c7 BOOLEAN, c8 BOOLEAN, c9 BOOLEAN,
             c10 TINYINT, c11 TINYINT, c12 TINYINT, c13 TINYINT, c14 TINYINT, c15 TINYINT, c16 TINYINT, c17 TINYINT, c18 TINYINT, c19 TINYINT, c20 TINYINT,
             c21 SMALLINT, c22 SMALLINT, c23 SMALLINT, c24 SMALLINT, c25 SMALLINT, c26 SMALLINT, c27 SMALLINT, c28 SMALLINT, c29 SMALLINT, c30 SMALLINT, c31 SMALLINT,
             c32 INT, c33 INT, c34 INT, c35 INT, c36 INT, c37 INT, c38 INT, c39 INT, c40 INT, c41 INT, c42 INT,
             c43 BIGINT, c44 BIGINT, c45 BIGINT, c46 BIGINT, c47 BIGINT, c48 BIGINT, c49 BIGINT, c50 BIGINT, c51 BIGINT, c52 BIGINT, c53 BIGINT,
             b STRING);

insert into table part_change_various_various_boolean_to_bigint partition(part=1) SELECT insert_num,
             boolean1, boolean1, boolean1, boolean1, boolean1, boolean1, boolean1, boolean1, boolean1,
             tinyint1, tinyint1, tinyint1, tinyint1, tinyint1, tinyint1, tinyint1, tinyint1, tinyint1, tinyint1, tinyint1,
             smallint1, smallint1, smallint1, smallint1, smallint1, smallint1, smallint1, smallint1, smallint1, smallint1, smallint1,
             int1, int1, int1, int1, int1, int1, int1, int1, int1, int1, int1,
             bigint1, bigint1, bigint1, bigint1, bigint1, bigint1, bigint1, bigint1, bigint1, bigint1, bigint1, 
              'new' FROM schema_evolution_data_n2;

explain vectorization detail
select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,c30,c31,c32,c33,c34,c35,c36,c37,c38,c39,c40,c41,c42,c43,c44,c45,c46,c47,c48,c49,c50,c51,c52,c53,b from part_change_various_various_boolean_to_bigint;

--** select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,c30,c31,c32,c33,c34,c35,c36,c37,c38,c39,c40,c41,c42,c43,c44,c45,c46,c47,c48,c49,c50,c51,c52,c53,b from part_change_various_various_boolean_to_bigint;

drop table part_change_various_various_boolean_to_bigint;



--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for Various --> Various:
--          (BOOLEAN, TINYINT, SMALLINT, INT, LONG, FLOAT, DOUBLE, STRING, CHAR, VARCHAR, TIMESTAMP) --> DECIMAL
--          (BOOLEAN, TINYINT, SMALLINT, INT, LONG, DOUBLE, DECIMAL, STRING, CHAR, VARCHAR, TIMESTAMP) --> FLOAT and
--          (BOOLEAN, TINYINT, SMALLINT, INT, LONG, FLOAT, DECIMAL, STRING, CHAR, VARCHAR, TIMESTAMP) --> DOUBLE and
--
CREATE TABLE part_change_various_various_decimal_to_double(insert_num int,
            c1 BOOLEAN, c2 TINYINT, c3 SMALLINT, c4 INT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 STRING, c9 CHAR(25), c10 VARCHAR(25), c11 TIMESTAMP,
            c12 BOOLEAN, c13 TINYINT, c14 SMALLINT, c15 INT, c16 BIGINT, c17 DECIMAL(38,18), c18 DOUBLE, c19 STRING, c20 CHAR(25), c21 VARCHAR(25), c22 TIMESTAMP,
            c23 BOOLEAN, c24 TINYINT, c25 SMALLINT, c26 INT, c27 BIGINT, c28 DECIMAL(38,18), c29 FLOAT, c30 STRING, c31 CHAR(25), c32 VARCHAR(25), c33 TIMESTAMP,
            b STRING) PARTITIONED BY(part INT);

insert into table part_change_various_various_decimal_to_double partition(part=1) SELECT insert_num,
            boolean1, tinyint1, smallint1, int1, bigint1, float1, double1, decimal_str, decimal_str, decimal_str, timestamp1,
            boolean1, tinyint1, smallint1, int1, bigint1, decimal1, double1, float_str, float_str, float_str, timestamp1,
            boolean1, tinyint1, smallint1, int1, bigint1, decimal1, float1, double_str, double_str, double_str, timestamp1,
            'original' FROM schema_evolution_data_n2;

explain vectorization detail
select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,c30,c31,c32,c33,b from part_change_various_various_decimal_to_double;

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,c30,c31,c32,c33,b from part_change_various_various_decimal_to_double;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table part_change_various_various_decimal_to_double replace columns (insert_num int,
             c1 DECIMAL(38,18), c2 DECIMAL(38,18), c3 DECIMAL(38,18), c4 DECIMAL(38,18), c5 DECIMAL(38,18), c6 DECIMAL(38,18), c7 DECIMAL(38,18), c8 DECIMAL(38,18), c9 DECIMAL(38,18), c10 DECIMAL(38,18), c11 DECIMAL(38,18), 
             c12 FLOAT, c13 FLOAT, c14 FLOAT, c15 FLOAT, c16 FLOAT, c17 FLOAT, c18 FLOAT, c19 FLOAT, c20 FLOAT, c21 FLOAT, c22 FLOAT,
             c23 DOUBLE, c24 DOUBLE, c25 DOUBLE, c26 DOUBLE, c27 DOUBLE, c28 DOUBLE, c29 DOUBLE, c30 DOUBLE, c31 DOUBLE, c32 DOUBLE, c33 DOUBLE,
             b STRING);

insert into table part_change_various_various_decimal_to_double partition(part=1) SELECT insert_num,
             decimal1, decimal1, decimal1, decimal1, decimal1, decimal1, decimal1, decimal1, decimal1, decimal1, decimal1,
             float1, float1, float1, float1, float1, float1, float1, float1, float1, float1, float1,
             double1, double1, double1, double1, double1, double1, double1, double1, double1, double1, double1,
             'new' FROM schema_evolution_data_2_n0 WHERE insert_num=111;

explain vectorization detail
select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,c30,c31,c32,c33,b from part_change_various_various_decimal_to_double;

--** select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,c30,c31,c32,c33,b from part_change_various_various_decimal_to_double;

drop table part_change_various_various_decimal_to_double;




--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for Various --> Various: (BOOLEAN, TINYINT, SMALLINT, INT, LONG, FLOAT, DOUBLE, DECIMAL(38,18), STRING, CHAR, VARCHAR, DATE) --> TIMESTAMP
--
CREATE TABLE part_change_various_various_timestamp(insert_num int, c1 BOOLEAN, c2 TINYINT, c3 SMALLINT, c4 INT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 DECIMAL(38,18), c9 STRING, c10 CHAR(25), c11 VARCHAR(25), c12 DATE, b STRING) PARTITIONED BY(part INT);

insert into table part_change_various_various_timestamp partition(part=1) SELECT insert_num, boolean1, tinyint1, smallint1, int1, bigint1, float1, double1, decimal1, timestamp_str, timestamp_str, timestamp_str, date1, 'original' FROM schema_evolution_data_n2;

explain vectorization detail
select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,b from part_change_various_various_timestamp;

--** select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,b from part_change_various_various_timestamp;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table part_change_various_various_timestamp replace columns (insert_num int, c1 TIMESTAMP, c2 TIMESTAMP, c3 TIMESTAMP, c4 TIMESTAMP, c5 TIMESTAMP, c6 TIMESTAMP, c7 TIMESTAMP, c8 TIMESTAMP, c9 TIMESTAMP, c10 TIMESTAMP, c11 TIMESTAMP, c12 TIMESTAMP, b STRING);

insert into table part_change_various_various_timestamp partition(part=1) SELECT insert_num, timestamp1, timestamp1, timestamp1, timestamp1, timestamp1, timestamp1, timestamp1, timestamp1, timestamp1, timestamp1, timestamp1, timestamp1, 'new' FROM schema_evolution_data_2_n0 WHERE insert_num=111;

explain vectorization detail
select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,b from part_change_various_various_timestamp;

--** select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,b from part_change_various_various_timestamp;

drop table part_change_various_various_timestamp;

--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for Various --> Various: (STRING, CHAR, VARCHAR, TIMESTAMP --> DATE
--
CREATE TABLE part_change_various_various_date(insert_num int, c1 STRING, c2 CHAR(25), c3 VARCHAR(25), c4 TIMESTAMP, b STRING) PARTITIONED BY(part INT);

insert into table part_change_various_various_date partition(part=1) SELECT insert_num, date_str, date_str, date_str, timestamp1, 'original' FROM schema_evolution_data_n2;

explain vectorization detail
select insert_num,part,c1,c2,c3,c4,b from part_change_various_various_date;

--** select insert_num,part,c1,c2,c3,c4,b from part_change_various_various_date;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table part_change_various_various_date replace columns (insert_num int, c1 DATE, c2 DATE, c3 DATE, c4 DATE, b STRING);

insert into table part_change_various_various_date partition(part=1) SELECT insert_num, date1, date1, date1, date1, 'new' FROM schema_evolution_data_2_n0 WHERE insert_num=111;

explain vectorization detail
select insert_num,part,c1,c2,c3,c4,b from part_change_various_various_date;

--** select insert_num,part,c1,c2,c3,c4,b from part_change_various_various_date;

drop table part_change_various_various_date;


--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for Same Type (CHAR, VARCHAR, DECIMAL) --> Different maxLength or precision/scale
--
CREATE TABLE part_change_same_type_different_params(insert_num int, c1 CHAR(12), c2 CHAR(25), c3 VARCHAR(25), c4 VARCHAR(10), c5 DECIMAL(12,4), c6 DECIMAL(20,10), b STRING) PARTITIONED BY(part INT);

CREATE TABLE same_type1_a_txt(insert_num int, c1 CHAR(12), c2 CHAR(25), c3 VARCHAR(25), c4 VARCHAR(10), c5 DECIMAL(12,4), c6 DECIMAL(20,10), b STRING)
row format delimited fields terminated by '|'
stored as textfile;
load data local inpath '../../data/files/schema_evolution/same_type1_a.txt' overwrite into table same_type1_a_txt;

insert into table part_change_same_type_different_params partition(part=1) select * from same_type1_a_txt;

explain vectorization detail
select insert_num,part,c1,c2,c3,c4,c5,c6,b from part_change_same_type_different_params;

--** select insert_num,part,c1,c2,c3,c4,c5,c6,b from part_change_same_type_different_params;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table part_change_same_type_different_params replace columns (insert_num int, c1 CHAR(8), c2 CHAR(32), c3 VARCHAR(15), c4 VARCHAR(18), c5 DECIMAL(10,2), c6 DECIMAL(25,15), b STRING);

CREATE TABLE same_type1_b_txt(insert_num int, c1 CHAR(8), c2 CHAR(32), c3 VARCHAR(15), c4 VARCHAR(18), c5 DECIMAL(10,2), c6 DECIMAL(25,15), b STRING)
row format delimited fields terminated by '|'
stored as textfile;
load data local inpath '../../data/files/schema_evolution/same_type1_b.txt' overwrite into table same_type1_b_txt;

insert into table part_change_same_type_different_params partition(part=1) select * from same_type1_b_txt;

CREATE TABLE same_type1_c_txt(insert_num int, c1 CHAR(8), c2 CHAR(32), c3 VARCHAR(15), c4 VARCHAR(18), c5 DECIMAL(10,2), c6 DECIMAL(25,15), b STRING)
row format delimited fields terminated by '|'
stored as textfile;
load data local inpath '../../data/files/schema_evolution/same_type1_c.txt' overwrite into table same_type1_c_txt;

insert into table part_change_same_type_different_params partition(part=2) select * from same_type1_c_txt;

explain vectorization detail
select insert_num,part,c1,c2,c3,c4,c5,c6,b from part_change_same_type_different_params;

--** select insert_num,part,c1,c2,c3,c4,c5,c6,b from part_change_same_type_different_params;

drop table part_change_same_type_different_params;
