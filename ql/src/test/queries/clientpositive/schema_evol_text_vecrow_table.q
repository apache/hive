set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.cli.print.header=true;
SET hive.exec.schema.evolution=true;
SET hive.vectorized.use.vectorized.input.format=false;
SET hive.vectorized.use.vector.serde.deserialize=false;
SET hive.vectorized.use.row.serde.deserialize=true;
SET hive.vectorized.execution.enabled=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.metastore.disallow.incompatible.col.type.changes=true;
set hive.default.fileformat=textfile;
set hive.llap.io.enabled=false;

-- SORT_QUERY_RESULTS
--
-- FILE VARIATION: TEXTFILE, Non-Vectorized, MapWork, Table
-- NOTE: the use of hive.vectorized.use.row.serde.deserialize above which enables doing
--  vectorized reading of TEXTFILE format files using the row SERDE methods.
--

CREATE TABLE schema_evolution_data_n24(insert_num int, boolean1 boolean, tinyint1 tinyint, smallint1 smallint, int1 int, bigint1 bigint, decimal1 decimal(38,18), float1 float, double1 double, string1 string, string2 string, date1 date, timestamp1 timestamp, boolean_str string, tinyint_str string, smallint_str string, int_str string, bigint_str string, decimal_str string, float_str string, double_str string, date_str string, timestamp_str string, filler string)
row format delimited fields terminated by '|' stored as textfile;
load data local inpath '../../data/files/schema_evolution/schema_evolution_data.txt' overwrite into table schema_evolution_data_n24;

------------------------------------------------------------------------------------------
-- SECTION: ALTER TABLE ADD COLUMNS
--
--
-- SUBSECTION: ALTER TABLE ADD COLUMNS: INT PERMUTE SELECT
--
--
CREATE TABLE table_add_int_permute_select_n8(insert_num int, a INT, b STRING);

insert into table table_add_int_permute_select_n8 SELECT insert_num, int1, 'original' FROM schema_evolution_data_n24;

-- Table-Non-Cascade ADD COLUMNS ...
alter table table_add_int_permute_select_n8 add columns(c int);

insert into table table_add_int_permute_select_n8 VALUES (111, 80000, 'new', 80000);

explain vectorization detail
select insert_num,a,b from table_add_int_permute_select_n8;

-- SELECT permutation columns to make sure NULL defaulting works right
select insert_num,a,b from table_add_int_permute_select_n8;
select insert_num,a,b,c from table_add_int_permute_select_n8;
select insert_num,c from table_add_int_permute_select_n8;

drop table table_add_int_permute_select_n8;


-- SUBSECTION: ALTER TABLE ADD COLUMNS: INT, STRING, PERMUTE SELECT
--
--
CREATE TABLE table_add_int_string_permute_select_n8(insert_num int, a INT, b STRING);

insert into table table_add_int_string_permute_select_n8 SELECT insert_num, int1, 'original' FROM schema_evolution_data_n24;

-- Table-Non-Cascade ADD COLUMNS ...
alter table table_add_int_string_permute_select_n8 add columns(c int, d string);

insert into table table_add_int_string_permute_select_n8 VALUES (111, 80000, 'new', 80000, 'filler');

explain vectorization detail
select insert_num,a,b from table_add_int_string_permute_select_n8;

-- SELECT permutation columns to make sure NULL defaulting works right
select insert_num,a,b from table_add_int_string_permute_select_n8;
select insert_num,a,b,c from table_add_int_string_permute_select_n8;
select insert_num,a,b,c,d from table_add_int_string_permute_select_n8;
select insert_num,a,c,d from table_add_int_string_permute_select_n8;
select insert_num,a,d from table_add_int_string_permute_select_n8;
select insert_num,c from table_add_int_string_permute_select_n8;
select insert_num,d from table_add_int_string_permute_select_n8;

drop table table_add_int_string_permute_select_n8;



------------------------------------------------------------------------------------------
-- SECTION: ALTER TABLE CHANGE COLUMNS for STRING_GROUP -> DOUBLE
--
--
--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for STRING_GROUP -> DOUBLE: (STRING, CHAR, VARCHAR)
--
CREATE TABLE table_change_string_group_double_n8(insert_num int, c1 STRING, c2 CHAR(50), c3 VARCHAR(50), b STRING);

insert into table table_change_string_group_double_n8 SELECT insert_num, double_str, double_str, double_str, 'original' FROM schema_evolution_data_n24;

-- Table-Non-Cascade CHANGE COLUMNS ...
set hive.metastore.disallow.incompatible.col.type.changes=false;
alter table table_change_string_group_double_n8 replace columns (insert_num int, c1 DOUBLE, c2 DOUBLE, c3 DOUBLE, b STRING);
set hive.metastore.disallow.incompatible.col.type.changes=true;

insert into table table_change_string_group_double_n8 VALUES (111, 789.321, 789.321, 789.321, 'new');

explain vectorization detail
select insert_num,c1,c2,c3,b from table_change_string_group_double_n8;

select insert_num,c1,c2,c3,b from table_change_string_group_double_n8;

drop table table_change_string_group_double_n8;

------------------------------------------------------------------------------------------
-- SECTION: ALTER TABLE CHANGE COLUMNS for DATE_GROUP -> STRING_GROUP
--
--
--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for DATE_GROUP -> STRING_GROUP: DATE,TIMESTAMP, (STRING, CHAR, CHAR trunc, VARCHAR, VARCHAR trunc)
--
CREATE TABLE table_change_date_group_string_group_date_group_n8(insert_num int, c1 DATE, c2 DATE, c3 DATE, c4 DATE, c5 DATE, c6 TIMESTAMP, c7 TIMESTAMP, c8 TIMESTAMP, c9 TIMESTAMP, c10 TIMESTAMP, b STRING);

insert into table table_change_date_group_string_group_date_group_n8 SELECT insert_num, date1, date1, date1, date1, date1, timestamp1, timestamp1, timestamp1, timestamp1, timestamp1, 'original' FROM schema_evolution_data_n24;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_date_group_string_group_date_group_n8 replace columns(insert_num int, c1 STRING, c2 CHAR(50), c3 CHAR(15), c4 VARCHAR(50), c5 VARCHAR(15), c6 STRING, c7 CHAR(50), c8 CHAR(15), c9 VARCHAR(50), c10 VARCHAR(15), b STRING);

insert into table table_change_date_group_string_group_date_group_n8 VALUES (111, 'filler', 'filler', 'filler', 'filler', 'filler', 'filler', 'filler', 'filler', 'filler', 'filler', 'new');

select insert_num,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,b from table_change_date_group_string_group_date_group_n8;

drop table table_change_date_group_string_group_date_group_n8;



------------------------------------------------------------------------------------------
-- SECTION: ALTER TABLE CHANGE COLUMNS for NUMERIC_GROUP -> STRING_GROUP
--
--
--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for NUMERIC_GROUP -> STRING_GROUP:
--           (TINYINT, SMALLINT, INT, BIGINT), STRING and
--           (TINYINT, SMALLINT, INT, BIGINT), CHAR and CHAR trunc and
--           (TINYINT, SMALLINT, INT, BIGINT), VARCHAR and VARCHAR trunc
--
--
CREATE TABLE table_change_numeric_group_string_group_multi_ints_string_group_n8(insert_num int,
             c1 tinyint, c2 smallint, c3 int, c4 bigint,
             c5 tinyint, c6 smallint, c7 int, c8 bigint, c9 tinyint, c10 smallint, c11 int, c12 bigint,
             c13 tinyint, c14 smallint, c15 int, c16 bigint, c17 tinyint, c18 smallint, c19 int, c20 bigint,
             b STRING);

insert into table table_change_numeric_group_string_group_multi_ints_string_group_n8 SELECT insert_num,
             tinyint1, smallint1, int1, bigint1,
             tinyint1, smallint1, int1, bigint1, tinyint1, smallint1, int1, bigint1,
             tinyint1, smallint1, int1, bigint1, tinyint1, smallint1, int1, bigint1,
             'original' FROM schema_evolution_data_n24;

select insert_num,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,b from table_change_numeric_group_string_group_multi_ints_string_group_n8;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_numeric_group_string_group_multi_ints_string_group_n8 replace columns (insert_num int,
             c1 STRING, c2 STRING, c3 STRING, c4 STRING,
             c5 CHAR(50), c6 CHAR(50), c7 CHAR(50), c8 CHAR(50), c9 CHAR(5), c10 CHAR(5), c11 CHAR(5), c12 CHAR(5),
             c13 VARCHAR(50), c14 VARCHAR(50), c15 VARCHAR(50), c16 VARCHAR(50), c17 VARCHAR(5), c18 VARCHAR(5), c19 VARCHAR(5), c20 VARCHAR(5),
             b STRING) ;

insert into table table_change_numeric_group_string_group_multi_ints_string_group_n8 VALUES (111,
            'filler', 'filler', 'filler', 'filler',
            'filler', 'filler', 'filler', 'filler', 'filler', 'filler', 'filler', 'filler',
            'filler', 'filler', 'filler', 'filler', 'filler', 'filler', 'filler', 'filler',
            'new');

explain vectorization detail
select insert_num,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,b from table_change_numeric_group_string_group_multi_ints_string_group_n8;

select insert_num,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,b from table_change_numeric_group_string_group_multi_ints_string_group_n8;

drop table table_change_numeric_group_string_group_multi_ints_string_group_n8;



--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for NUMERIC_GROUP -> STRING_GROUP:
--            (DECIMAL, FLOAT, DOUBLE), STRING and
--            (DECIMAL, FLOAT, DOUBLE), CHAR and CHAR trunc and
--            (DECIMAL, FLOAT, DOUBLE), VARCHAR and VARCHAR trunc
--
--
CREATE TABLE table_change_numeric_group_string_group_floating_string_group_n8(insert_num int,
              c1 decimal(38,18), c2 float, c3 double,
              c4 decimal(38,18), c5 float, c6 double, c7 decimal(38,18), c8 float, c9 double,
              c10 decimal(38,18), c11 float, c12 double, c13 decimal(38,18), c14 float, c15 double,
              b STRING);

insert into table table_change_numeric_group_string_group_floating_string_group_n8 SELECT insert_num,
              decimal1, float1, double1,
              decimal1, float1, double1, decimal1, float1, double1,
              decimal1, float1, double1, decimal1, float1, double1,
             'original' FROM schema_evolution_data_n24;

select insert_num,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,b from table_change_numeric_group_string_group_floating_string_group_n8;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_numeric_group_string_group_floating_string_group_n8 replace columns (insert_num int,
              c1 STRING, c2 STRING, c3 STRING,
              c4 CHAR(50), c5 CHAR(50), c6 CHAR(50), c7 CHAR(7), c8 CHAR(7), c9 CHAR(7),
              c10 VARCHAR(50), c11 VARCHAR(50), c12 VARCHAR(50), c13 VARCHAR(7), c14 VARCHAR(7), c15 VARCHAR(7),
              b STRING);

insert into table table_change_numeric_group_string_group_floating_string_group_n8 VALUES (111,
             'filler', 'filler', 'filler',
             'filler', 'filler', 'filler', 'filler', 'filler', 'filler',
             'filler', 'filler', 'filler', 'filler', 'filler', 'filler',
             'new');

explain vectorization detail
select insert_num,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,b from table_change_numeric_group_string_group_floating_string_group_n8;

select insert_num,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,b from table_change_numeric_group_string_group_floating_string_group_n8;

drop table table_change_numeric_group_string_group_floating_string_group_n8;


------------------------------------------------------------------------------------------
-- SECTION: ALTER TABLE CHANGE COLUMNS for STRING_GROUP -> STRING_GROUP
--
--
--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for STRING_GROUP -> STRING_GROUP: STRING, (CHAR, CHAR trunc, VARCHAR, VARCHAR trunc) and
--      CHAR, (VARCHAR, VARCHAR trunc, STRING) and VARCHAR, (CHAR, CHAR trunc, STRING)
--
CREATE TABLE table_change_string_group_string_group_string_n8(insert_num int,
           c1 string, c2 string, c3 string, c4 string,
           c5 CHAR(50), c6 CHAR(50), c7 CHAR(50),
           c8 VARCHAR(50), c9 VARCHAR(50), c10 VARCHAR(50), b STRING);

insert into table table_change_string_group_string_group_string_n8 SELECT insert_num,
           string2, string2, string2, string2,
           string2, string2, string2,
           string2, string2, string2,
          'original' FROM schema_evolution_data_n24;

select insert_num,c1,c2,c3,c4,b from table_change_string_group_string_group_string_n8;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_string_group_string_group_string_n8 replace columns (insert_num int,
           c1 CHAR(50), c2 CHAR(9), c3 VARCHAR(50), c4 CHAR(9),
           c5 VARCHAR(50), c6 VARCHAR(9), c7 STRING,
           c8 CHAR(50), c9 CHAR(9), c10 STRING, b STRING) ;

insert into table table_change_string_group_string_group_string_n8 VALUES (111,
          'filler', 'filler', 'filler', 'filler',
          'filler', 'filler', 'filler',
          'filler', 'filler', 'filler',
          'new');

select insert_num,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,b from table_change_string_group_string_group_string_n8;

drop table table_change_string_group_string_group_string_n8;



------------------------------------------------------------------------------------------
-- SECTION: ALTER TABLE CHANGE COLUMNS for "lower" type to "higher" NUMERIC_GROUP
--
--
--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for "lower" type to "higher" NUMERIC_GROUP:
--          TINYINT, (SMALLINT, INT, BIGINT, DECIMAL, FLOAT, DOUBLE) and
--          SMALLINT, (INT, BIGINT, DECIMAL, FLOAT, DOUBLE) and
--          INT, (BIGINT, DECIMAL, FLOAT, DOUBLE) and
--          BIGINT, (DECIMAL, FLOAT, DOUBLE)
--
CREATE TABLE table_change_lower_to_higher_numeric_group_tinyint_to_bigint_n8(insert_num int,
                                c1 tinyint, c2 tinyint, c3 tinyint, c4 tinyint, c5 tinyint, c6 tinyint,
                                c7 smallint, c8 smallint, c9 smallint, c10 smallint, c11 smallint,
                                c12 int, c13 int, c14 int, c15 int,
                                c16 bigint, c17 bigint, c18 bigint,
                                b STRING);

insert into table table_change_lower_to_higher_numeric_group_tinyint_to_bigint_n8 SELECT insert_num,
                                tinyint1, tinyint1, tinyint1, tinyint1, tinyint1, tinyint1,
                                smallint1, smallint1, smallint1, smallint1, smallint1,
                                int1, int1, int1, int1,
                                bigint1, bigint1, bigint1, 
                                'original' FROM schema_evolution_data_n24;

select insert_num,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,b from table_change_lower_to_higher_numeric_group_tinyint_to_bigint_n8;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_lower_to_higher_numeric_group_tinyint_to_bigint_n8 replace columns (insert_num int,
             c1 SMALLINT, c2 INT, c3 BIGINT, c4 decimal(38,18), c5 FLOAT, c6 DOUBLE,
             c7 INT, c8 BIGINT, c9 decimal(38,18), c10 FLOAT, c11 DOUBLE,
             c12 BIGINT, c13 decimal(38,18), c14 FLOAT, c15 DOUBLE,
             c16 decimal(38,18), c17 FLOAT, c18 DOUBLE,
             b STRING) ;

insert into table table_change_lower_to_higher_numeric_group_tinyint_to_bigint_n8 VALUES (111,
            7000, 80000, 90000000, 1234.5678, 9876.543, 789.321,
            80000, 90000000, 1234.5678, 9876.543, 789.321,
            90000000, 1234.5678, 9876.543, 789.321,
            1234.5678, 9876.543, 789.321,
           'new');

select insert_num,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,b from table_change_lower_to_higher_numeric_group_tinyint_to_bigint_n8;

drop table table_change_lower_to_higher_numeric_group_tinyint_to_bigint_n8;



--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for "lower" type to "higher" NUMERIC_GROUP:
--          DECIMAL, (FLOAT, DOUBLE) and
--          FLOAT, (DOUBLE)
--
CREATE TABLE table_change_lower_to_higher_numeric_group_decimal_to_float_n8(insert_num int,
           c1 decimal(38,18), c2 decimal(38,18),
           c3 float,
           b STRING);

insert into table table_change_lower_to_higher_numeric_group_decimal_to_float_n8 SELECT insert_num,
           decimal1, decimal1,
           float1,
          'original' FROM schema_evolution_data_n24;

select insert_num,c1,c2,c3,b from table_change_lower_to_higher_numeric_group_decimal_to_float_n8;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_lower_to_higher_numeric_group_decimal_to_float_n8 replace columns (insert_num int, c1 float, c2 double, c3 DOUBLE, b STRING) ;

insert into table table_change_lower_to_higher_numeric_group_decimal_to_float_n8 VALUES (111, 1234.5678, 9876.543, 1234.5678, 'new');

select insert_num,c1,c2,c3,b from table_change_lower_to_higher_numeric_group_decimal_to_float_n8;

drop table table_change_lower_to_higher_numeric_group_decimal_to_float_n8;
