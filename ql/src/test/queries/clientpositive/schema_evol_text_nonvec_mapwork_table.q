set hive.explain.user=true;
set hive.cli.print.header=true;
SET hive.exec.schema.evolution=true;
SET hive.vectorized.use.vectorized.input.format=true;
SET hive.vectorized.use.vector.serde.deserialize=false;
SET hive.vectorized.use.row.serde.deserialize=false;
set hive.fetch.task.conversion=none;
SET hive.vectorized.execution.enabled=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.metastore.disallow.incompatible.col.type.changes=true;
set hive.default.fileformat=textfile;

-- SORT_QUERY_RESULTS
--
-- FILE VARIATION: TEXTFILE, Non-Vectorized, MapWork, Table
--
------------------------------------------------------------------------------------------
-- SECTION: ALTER TABLE ADD COLUMNS
--
--
-- SUBSECTION: ALTER TABLE ADD COLUMNS: INT PERMUTE SELECT
--
--
CREATE TABLE table_add_int_permute_select(insert_num int, a INT, b STRING);
DESCRIBE FORMATTED table_add_int_permute_select;

insert into table table_add_int_permute_select
    values (1, 1, 'original'),
           (2, 2, 'original'),
           (3, 3, 'original'),
           (4, 4, 'original');

-- Table-Non-Cascade ADD COLUMNS ...
alter table table_add_int_permute_select add columns(c int);
DESCRIBE FORMATTED table_add_int_permute_select;

insert into table table_add_int_permute_select
    values (5, 1, 'new', 10),
           (6, 2, 'new', 20),
           (7, 3, 'new', 30),
           (8, 4, 'new', 40);

insert into table table_add_int_permute_select
    values (9, 5, 'new', 100),
           (10, 6, 'new', 200);

explain
select insert_num,a,b from table_add_int_permute_select order by insert_num;

-- SELECT permutation columns to make sure NULL defaulting works right
select insert_num,a,b from table_add_int_permute_select order by insert_num;
select insert_num,a,b,c from table_add_int_permute_select order by insert_num;
select insert_num,c from table_add_int_permute_select order by insert_num;

drop table table_add_int_permute_select;


-- SUBSECTION: ALTER TABLE ADD COLUMNS: INT, STRING, PERMUTE SELECT
--
--
CREATE TABLE table_add_int_string_permute_select(insert_num int, a INT, b STRING);
DESCRIBE FORMATTED table_add_int_string_permute_select;

insert into table table_add_int_string_permute_select
    values (1, 1, 'original'),
           (2, 2, 'original'),
           (3, 3, 'original'),
           (4, 4, 'original');

-- Table-Non-Cascade ADD COLUMNS ...
alter table table_add_int_string_permute_select add columns(c int, d string);
DESCRIBE FORMATTED table_add_int_string_permute_select;

insert into table table_add_int_string_permute_select
    values (5, 1, 'new', 10, 'ten'),
           (6, 2, 'new', 20, 'twenty'),
           (7, 3, 'new', 30, 'thirty'),
           (8, 4, 'new', 40, 'forty');

insert into table table_add_int_string_permute_select
    values (9, 5, 'new', 100, 'hundred'),
           (10, 6, 'new', 200, 'two hundred');

explain
select insert_num,a,b from table_add_int_string_permute_select order by insert_num;

-- SELECT permutation columns to make sure NULL defaulting works right
select insert_num,a,b from table_add_int_string_permute_select order by insert_num;
select insert_num,a,b,c from table_add_int_string_permute_select order by insert_num;
select insert_num,a,b,c,d from table_add_int_string_permute_select order by insert_num;
select insert_num,a,c,d from table_add_int_string_permute_select order by insert_num;
select insert_num,a,d from table_add_int_string_permute_select order by insert_num;
select insert_num,c from table_add_int_string_permute_select order by insert_num;
select insert_num,d from table_add_int_string_permute_select order by insert_num;

drop table table_add_int_string_permute_select;



------------------------------------------------------------------------------------------
-- SECTION: ALTER TABLE CHANGE COLUMNS for STRING_GROUP -> DOUBLE
--
--
--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for STRING_GROUP -> DOUBLE: (STRING, CHAR, VARCHAR)
--
CREATE TABLE table_change_string_group_double(insert_num int, c1 STRING, c2 CHAR(50), c3 VARCHAR(50), b STRING);

insert into table table_change_string_group_double
    values (1, '753.7028', '753.7028', '753.7028', 'original'),
           (2, '-3651.672121', '-3651.672121', '-3651.672121', 'original'),
           (3, '-29.0764', '-29.0764', '-29.0764', 'original'),
           (4, '-10.3', '-10.3', '-10.3', 'original');

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_string_group_double replace columns (insert_num int, c1 DOUBLE, c2 DOUBLE, c3 DOUBLE, b STRING);

insert into table table_change_string_group_double
    values (5, 30.774, 30.774, 30.774, 'new'),
           (6, 20.31, 20.31, 20.31, 'new'),
           (7, 46114.284799488, 46114.284799488, 46114.284799488, 'new'),
           (8, -66475.561431, -66475.561431, -66475.561431, 'new');

insert into table table_change_string_group_double
    values (9, 17808.963785, 17808.963785, 17808.963785, 'new'),
           (10, 9250340.75 , 9250340.75 , 9250340.75 , 'new');

explain
select insert_num,c1,c2,c3,b from table_change_string_group_double order by insert_num;

select insert_num,c1,c2,c3,b from table_change_string_group_double order by insert_num;

drop table table_change_string_group_double;

------------------------------------------------------------------------------------------
-- SECTION: ALTER TABLE CHANGE COLUMNS for DATE_GROUP -> STRING_GROUP
--
--
--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for DATE_GROUP -> STRING_GROUP: TIMESTAMP, (STRING, CHAR, CHAR trunc, VARCHAR, VARCHAR trunc)
--
CREATE TABLE table_change_date_group_string_group_timestamp(insert_num int, c1 TIMESTAMP, c2 TIMESTAMP, c3 TIMESTAMP, c4 TIMESTAMP, c5 TIMESTAMP, b STRING);

insert into table table_change_date_group_string_group_timestamp
    values (1, '2000-12-18 08:42:30.000595596', '2000-12-18 08:42:30.000595596', '2000-12-18 08:42:30.000595596', '2000-12-18 08:42:30.000595596', '2000-12-18 08:42:30.000595596', 'original'),
           (2, '2024-11-11 16:42:41.101', '2024-11-11 16:42:41.101', '2024-11-11 16:42:41.101', '2024-11-11 16:42:41.101', '2024-11-11 16:42:41.101', 'original'),
           (3, '2021-09-24 03:18:32.413655165', '2021-09-24 03:18:32.413655165', '2021-09-24 03:18:32.413655165', '2021-09-24 03:18:32.413655165', '2021-09-24 03:18:32.413655165', 'original');

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_date_group_string_group_timestamp replace columns(insert_num int, c1 STRING, c2 CHAR(50), c3 CHAR(15), c4 VARCHAR(50), c5 VARCHAR(15), b STRING);

insert into table table_change_date_group_string_group_timestamp
    values (4, '0004-09-22 18:26:29.519542222', '0004-09-22 18:26:29.519542222', '0004-09-22 18:26:29.519542222', '0004-09-22 18:26:29.519542222', '0004-09-22 18:26:29.519542222', 'new'),
           (5, '1815-05-06 00:12:37.543584705', '1815-05-06 00:12:37.543584705', '1815-05-06 00:12:37.543584705', '1815-05-06 00:12:37.543584705', '1815-05-06 00:12:37.543584705', 'new'),
           (6, '2007-02-09 05:17:29.368756876', '2007-02-09 05:17:29.368756876', '2007-02-09 05:17:29.368756876', '2007-02-09 05:17:29.368756876', '2007-02-09 05:17:29.368756876', 'new'),
           (7, '2002-05-10 05:29:48.990818073', '2002-05-10 05:29:48.990818073', '2002-05-10 05:29:48.990818073', '2002-05-10 05:29:48.990818073', '2002-05-10 05:29:48.990818073', 'new');
insert into table table_change_date_group_string_group_timestamp
    values (8, '6229-06-28 02:54:28.970117179', '6229-06-28 02:54:28.970117179', '6229-06-28 02:54:28.970117179', '6229-06-28 02:54:28.970117179', '6229-06-28 02:54:28.970117179', 'new'),
           (9, '5966-07-09 03:30:50.597', '5966-07-09 03:30:50.597', '5966-07-09 03:30:50.597', '5966-07-09 03:30:50.597', '5966-07-09 03:30:50.597', 'new');

explain
select insert_num,c1,c2,c3,c4,c5,b from table_change_date_group_string_group_timestamp order by insert_num;

select insert_num,c1,c2,c3,c4,c5,b from table_change_date_group_string_group_timestamp order by insert_num;

drop table table_change_date_group_string_group_timestamp;



--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for DATE_GROUP -> STRING_GROUP: DATE, (STRING, CHAR, CHAR trunc, VARCHAR, VARCHAR trunc)
--
CREATE TABLE table_change_date_group_string_group_date(insert_num int, c1 DATE, c2 DATE, c3 DATE, c4 DATE, c5 DATE, b STRING);

insert into table table_change_date_group_string_group_date
    values (1, '2000-12-18', '2000-12-18', '2000-12-18', '2000-12-18', '2000-12-18', 'original'),
           (2, '2024-11-11', '2024-11-11', '2024-11-11', '2024-11-11', '2024-11-11', 'original'),
           (3, '2021-09-24', '2021-09-24', '2021-09-24', '2021-09-24', '2021-09-24', 'original');

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_date_group_string_group_date replace columns(insert_num int, c1 STRING, c2 CHAR(50), c3 CHAR(15), c4 VARCHAR(50), c5 VARCHAR(15), b STRING);DESCRIBE FORMATTED table_change_date_group_string_group_date;

insert into table table_change_date_group_string_group_date
    values (4, '0004-09-22', '0004-09-22', '0004-09-22', '0004-09-22', '0004-09-22', 'new'),
           (5, '1815-05-06', '1815-05-06', '1815-05-06', '1815-05-06', '1815-05-06', 'new'),
           (6, '2007-02-09', '2007-02-09', '2007-02-09', '2007-02-09', '2007-02-09', 'new'),
           (7, '2002-05-10', '2002-05-10', '2002-05-10', '2002-05-10', '2002-05-10', 'new'),
           (8, '6229-06-28', '6229-06-28', '6229-06-28', '6229-06-28', '6229-06-28', 'new'),
           (9, '5966-07-09', '5966-07-09', '5966-07-09', '5966-07-09', '5966-07-09', 'new');

explain
select insert_num,c1,c2,c3,c4,c5,b from table_change_date_group_string_group_date order by insert_num;

select insert_num,c1,c2,c3,c4,c5,b from table_change_date_group_string_group_date order by insert_num;

drop table table_change_date_group_string_group_date;



------------------------------------------------------------------------------------------
-- SECTION: ALTER TABLE CHANGE COLUMNS for NUMERIC_GROUP -> STRING_GROUP
--
--
--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for NUMERIC_GROUP -> STRING_GROUP: (TINYINT, SMALLINT, INT, BIGINT), STRING
--
CREATE TABLE table_change_numeric_group_string_group_multi_ints_string(insert_num int, c1 tinyint, c2 smallint, c3 int, c4 bigint, b STRING);

insert into table table_change_numeric_group_string_group_multi_ints_string
    values (1, 45, 1000, 483777, -23866739993, 'original'),
           (2, -2, -6737, 56, 28899333, 'original'),
           (3, -255, 4957, 832222222, 9000000000, 'original'),
           (4, 0, 20435, 847492223, -999999999999, 'original');

select insert_num,c1,c2,c3,c4,b from table_change_numeric_group_string_group_multi_ints_string order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_numeric_group_string_group_multi_ints_string replace columns (insert_num int, c1 STRING, c2 STRING, c3 STRING, c4 STRING, b STRING) ;

insert into table table_change_numeric_group_string_group_multi_ints_string
    values (5, '2000', '72909', '3244222', '-93222', 'new'),
           (6, '1', '200', '2323322', '5430907', 'new'),
           (7, '256', '32768', '31889', '470614135', 'new'),
           (8, '5555', '40000', '-719017797', '810662019', 'new'),
           (9, '100', '5000', '5443', '0', 'new'),
           (10, '17', '90000', '754072151', '3289094', 'new');

explain
select insert_num,c1,c2,c3,c4,b from table_change_numeric_group_string_group_multi_ints_string order by insert_num;

select insert_num,c1,c2,c3,c4,b from table_change_numeric_group_string_group_multi_ints_string order by insert_num;

drop table table_change_numeric_group_string_group_multi_ints_string;



--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for NUMERIC_GROUP -> STRING_GROUP: (TINYINT, SMALLINT, INT, BIGINT), CHAR
--
CREATE TABLE table_change_numeric_group_string_group_multi_ints_char(insert_num int, c1 tinyint, c2 smallint, c3 int, c4 bigint, b STRING);

insert into table table_change_numeric_group_string_group_multi_ints_char
    values (1, 45, 1000, 483777, -23866739993, 'original'),
           (2, -2, -6737, 56, 28899333, 'original'),
           (3, -255, 4957, 832222222, 9000000000, 'original'),
           (4, 0, 20435, 847492223, -999999999999, 'original');

select insert_num,c1,c2,c3,c4,b from table_change_numeric_group_string_group_multi_ints_char order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_numeric_group_string_group_multi_ints_char replace columns (insert_num int, c1 CHAR(50), c2 CHAR(50), c3 CHAR(50), c4 CHAR(50), b STRING) ;

insert into table table_change_numeric_group_string_group_multi_ints_char
    values (5, '2000', '72909', '3244222', '-93222', 'new'),
           (6, '1', '200', '2323322', '5430907', 'new'),
           (7, '256', '32768', '31889', '470614135', 'new'),
           (8, '5555', '40000', '-719017797', '810662019', 'new'),
           (9, '100', '5000', '5443', '0', 'new'),
           (10, '17', '90000', '754072151', '3289094', 'new');

explain
select insert_num,c1,c2,c3,c4,b from table_change_numeric_group_string_group_multi_ints_char order by insert_num;

select insert_num,c1,c2,c3,c4,b from table_change_numeric_group_string_group_multi_ints_char order by insert_num;

drop table table_change_numeric_group_string_group_multi_ints_char;


--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for NUMERIC_GROUP -> STRING_GROUP: (TINYINT, SMALLINT, INT, BIGINT), CHAR truncation
--
CREATE TABLE table_change_numeric_group_string_group_multi_ints_char_trunc(insert_num int, c1 tinyint, c2 smallint, c3 int, c4 bigint, b STRING);

insert into table table_change_numeric_group_string_group_multi_ints_char_trunc
    values (1, 45, 1000, 483777, -23866739993, 'original'),
           (2, -2, -6737, 56, 28899333, 'original'),
           (3, -255, 4957, 832222222, 9000000000, 'original'),
           (4, 0, 20435, 847492223, -999999999999, 'original');

select insert_num,c1,c2,c3,c4,b from table_change_numeric_group_string_group_multi_ints_char_trunc order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_numeric_group_string_group_multi_ints_char_trunc replace columns (insert_num int, c1 CHAR(5), c2 CHAR(5), c3 CHAR(5), c4 CHAR(5), b STRING) ;

insert into table table_change_numeric_group_string_group_multi_ints_char_trunc
    values (5, '2000', '72909', '3244222', '-93222', 'new'),
           (6, '1', '200', '2323322', '5430907', 'new'),
           (7, '256', '32768', '31889', '470614135', 'new'),
           (8, '5555', '40000', '-719017797', '810662019', 'new'),
           (9, '100', '5000', '5443', '0', 'new'),
           (10, '17', '90000', '754072151', '3289094', 'new');

explain
select insert_num,c1,c2,c3,c4,b from table_change_numeric_group_string_group_multi_ints_char_trunc order by insert_num;

select insert_num,c1,c2,c3,c4,b from table_change_numeric_group_string_group_multi_ints_char_trunc order by insert_num;

drop table table_change_numeric_group_string_group_multi_ints_char_trunc;



--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for NUMERIC_GROUP -> STRING_GROUP: (TINYINT, SMALLINT, INT, BIGINT), VARCHAR
--
CREATE TABLE table_change_numeric_group_string_group_multi_ints_varchar(insert_num int, c1 tinyint, c2 smallint, c3 int, c4 bigint, b STRING);

insert into table table_change_numeric_group_string_group_multi_ints_varchar
    values (1, 45, 1000, 483777, -23866739993, 'original'),
           (2, -2, -6737, 56, 28899333, 'original'),
           (3, -255, 4957, 832222222, 9000000000, 'original'),
           (4, 0, 20435, 847492223, -999999999999, 'original');

select insert_num,c1,c2,c3,c4,b from table_change_numeric_group_string_group_multi_ints_varchar order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_numeric_group_string_group_multi_ints_varchar replace columns (insert_num int, c1 VARCHAR(50), c2 VARCHAR(50), c3 VARCHAR(50), c4 VARCHAR(50), b STRING) ;

insert into table table_change_numeric_group_string_group_multi_ints_varchar
    values (5, '2000', '72909', '3244222', '-93222', 'new'),
           (6, '1', '200', '2323322', '5430907', 'new'),
           (7, '256', '32768', '31889', '470614135', 'new'),
           (8, '5555', '40000', '-719017797', '810662019', 'new'),
           (9, '100', '5000', '5443', '0', 'new'),
           (10, '17', '90000', '754072151', '3289094', 'new');

explain
select insert_num,c1,c2,c3,c4,b from table_change_numeric_group_string_group_multi_ints_varchar order by insert_num;

select insert_num,c1,c2,c3,c4,b from table_change_numeric_group_string_group_multi_ints_varchar order by insert_num;

drop table table_change_numeric_group_string_group_multi_ints_varchar;


--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for NUMERIC_GROUP -> STRING_GROUP: (TINYINT, SMALLINT, INT, BIGINT), VARCHAR truncation
--
CREATE TABLE table_change_numeric_group_string_group_multi_ints_varchar_trunc(insert_num int, c1 tinyint, c2 smallint, c3 int, c4 bigint, b STRING);

insert into table table_change_numeric_group_string_group_multi_ints_varchar_trunc
    values (1, 45, 1000, 483777, -23866739993, 'original'),
           (2, -2, -6737, 56, 28899333, 'original'),
           (3, -255, 4957, 832222222, 9000000000, 'original'),
           (4, 0, 20435, 847492223, -999999999999, 'original');

select insert_num,c1,c2,c3,c4,b from table_change_numeric_group_string_group_multi_ints_varchar_trunc order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_numeric_group_string_group_multi_ints_varchar_trunc replace columns (insert_num int, c1 VARCHAR(5), c2 VARCHAR(5), c3 VARCHAR(5), c4 VARCHAR(5), b STRING) ;

insert into table table_change_numeric_group_string_group_multi_ints_varchar_trunc
    values (5, '2000', '72909', '3244222', '-93222', 'new'),
           (6, '1', '200', '2323322', '5430907', 'new'),
           (7, '256', '32768', '31889', '470614135', 'new'),
           (8, '5555', '40000', '-719017797', '810662019', 'new'),
           (9, '100', '5000', '5443', '0', 'new'),
           (10, '17', '90000', '754072151', '3289094', 'new');

explain
select insert_num,c1,c2,c3,c4,b from table_change_numeric_group_string_group_multi_ints_varchar_trunc order by insert_num;

select insert_num,c1,c2,c3,c4,b from table_change_numeric_group_string_group_multi_ints_varchar_trunc order by insert_num;

drop table table_change_numeric_group_string_group_multi_ints_varchar_trunc;


--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for NUMERIC_GROUP -> STRING_GROUP: (FLOAT, DOUBLE, DECIMAL), STRING
--
CREATE TABLE table_change_numeric_group_string_group_floating_string(insert_num int, c1 decimal(38,18), c2 float, c3 double, b STRING);

insert into table table_change_numeric_group_string_group_floating_string
    values (1, -23866739993, 753.7028, -3651.672121, 'original'),
           (2, -10.3, -2, -29.0764, 'original'),
           (3, - 832222222, 255, 4957,'original'),
           (4, 847492223, 0, 20435, 'original');

select insert_num,c1,c2,c3,b from table_change_numeric_group_string_group_floating_string order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_numeric_group_string_group_floating_string replace columns (insert_num int, c1 STRING, c2 STRING, c3 STRING, b STRING) ;

insert into table table_change_numeric_group_string_group_floating_string
    values (5, '30.774', '20.31', '46114.284799488', 'new'),
           (6, '-66475.561431', '52927714', '7203778961', 'new'),
           (7, '256', '32768', '31889', 'new'),
           (8, '5555', '40000', '-719017797', 'new'),
           (9, '100', '5000', '5443', 'new'),
           (10, '17', '90000', '754072151', 'new');

explain
select insert_num,c1,c2,c3,b from table_change_numeric_group_string_group_floating_string order by insert_num;

select insert_num,c1,c2,c3,b from table_change_numeric_group_string_group_floating_string order by insert_num;

drop table table_change_numeric_group_string_group_floating_string;


--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for NUMERIC_GROUP -> STRING_GROUP: (DECIMAL, FLOAT, DOUBLE), CHAR
--
CREATE TABLE table_change_numeric_group_string_group_floating_char(insert_num int, c1 decimal(38,18), c2 float, c3 double, b STRING);

insert into table table_change_numeric_group_string_group_floating_char
    values (1, -23866739993, 753.7028, -3651.672121, 'original'),
           (2, -10.3, -2, -29.0764, 'original'),
           (3,  9000000000, -255, 4957,'original'),
           (4, -999999999999, 0, 20435, 'original');

select insert_num,c1,c2,c3,b from table_change_numeric_group_string_group_floating_char order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_numeric_group_string_group_floating_char replace columns (insert_num int, c1 CHAR(50), c2 CHAR(50), c3 CHAR(50), b STRING) ;

insert into table table_change_numeric_group_string_group_floating_char
    values (5, '30.774', '20.31', '46114.284799488', 'new'),
           (6, '-66475.561431', '52927714', '7203778961', 'new'),
           (7, '256', '32768', '31889', 'new'),
           (8, '5555', '40000', '-719017797', 'new'),
           (9, '100', '5000', '5443', 'new'),
           (10, '17', '90000', '754072151', 'new');

explain
select insert_num,c1,c2,c3,b from table_change_numeric_group_string_group_floating_char order by insert_num;

select insert_num,c1,c2,c3,b from table_change_numeric_group_string_group_floating_char order by insert_num;

drop table table_change_numeric_group_string_group_floating_char;


--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for NUMERIC_GROUP -> STRING_GROUP: (DECIMAL, FLOAT, DOUBLE), CHAR truncation
--
CREATE TABLE table_change_numeric_group_string_group_floating_char_trunc(insert_num int, c1 decimal(38,18), c2 float, c3 double, b STRING);

insert into table table_change_numeric_group_string_group_floating_char_trunc
    values (1, -23866739993, 753.7028, -3651.672121, 'original'),
           (2, -10.3, -2, -29.0764, 'original'),
           (3, 832222222, -255, 4957, 'original'),
           (4, 847492223, 0, 20435, 'original');

select insert_num,c1,c2,c3,b from table_change_numeric_group_string_group_floating_char_trunc order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_numeric_group_string_group_floating_char_trunc replace columns (insert_num int, c1 CHAR(7), c2 CHAR(7), c3 CHAR(7), b STRING) ;

insert into table table_change_numeric_group_string_group_floating_char_trunc
    values (5, '30.774', '20.31', '46114.284799488', 'new'),
           (6, '-66475.561431', '52927714', '7203778961', 'new'),
           (7, '256', '32768', '31889', 'new'),
           (8, '5555', '40000', '-719017797', 'new'),
           (9, '100', '5000', '5443', 'new'),
           (10, '17', '90000', '754072151', 'new');

explain
select insert_num,c1,c2,c3,b from table_change_numeric_group_string_group_floating_char_trunc order by insert_num;

select insert_num,c1,c2,c3,b from table_change_numeric_group_string_group_floating_char_trunc order by insert_num;

drop table table_change_numeric_group_string_group_floating_char_trunc;


--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for NUMERIC_GROUP -> STRING_GROUP: (DECIMAL, FLOAT, DOUBLE), VARCHAR
--
CREATE TABLE table_change_numeric_group_string_group_floating_varchar(insert_num int, c1 float, c2 double, c3 decimal(38,18), b STRING);

insert into table table_change_numeric_group_string_group_floating_varchar
    values (1, -23866739993, 753.7028, -3651.672121, 'original'),
           (2, -10.3, -2, -29.0764, 'original'),
           (3, 9000000000, -255, 4957, 'original'),
           (4, -999999999999, 0, 20435, 'original');

select insert_num,c1,c2,c3,b from table_change_numeric_group_string_group_floating_varchar order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_numeric_group_string_group_floating_varchar replace columns (insert_num int, c1 VARCHAR(50), c2 VARCHAR(50), c3 VARCHAR(50), b STRING) ;

insert into table table_change_numeric_group_string_group_floating_varchar
    values (5, '30.774', '20.31', '46114.284799488', 'new'),
           (6, '-66475.561431', '52927714', '7203778961', 'new'),
           (7, '256', '32768', '31889', 'new'),
           (8, '5555', '40000', '-719017797', 'new'),
           (9, '100', '5000', '5443', 'new'),
           (10, '17', '90000', '754072151', 'new');

explain
select insert_num,c1,c2,c3,b from table_change_numeric_group_string_group_floating_varchar order by insert_num;

select insert_num,c1,c2,c3,b from table_change_numeric_group_string_group_floating_varchar order by insert_num;

drop table table_change_numeric_group_string_group_floating_varchar;


--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for NUMERIC_GROUP -> STRING_GROUP: (DECIMAL, FLOAT, DOUBLE), VARCHAR truncation
--
CREATE TABLE table_change_numeric_group_string_group_floating_varchar_trunc(insert_num int, c1 decimal(38,18), c2 float, c3 double, b STRING);

insert into table table_change_numeric_group_string_group_floating_varchar_trunc
    values (1, -23866739993, 753.7028, -3651.672121, 'original'),
           (2, -10.3, -2, -29.0764, 'original'),
           (3, 9000000000, -255, 4957, 'original'),
           (4, -999999999999, 0, 20435, 'original');

select insert_num,c1,c2,c3,b from table_change_numeric_group_string_group_floating_varchar_trunc order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_numeric_group_string_group_floating_varchar_trunc replace columns (insert_num int, c1 VARCHAR(7), c2 VARCHAR(7), c3 VARCHAR(7), b STRING) ;

insert into table table_change_numeric_group_string_group_floating_varchar_trunc
    values (5, '30.774', '20.31', '46114.284799488', 'new'),
           (6, '-66475.561431', '52927714', '7203778961', 'new'),
           (7, '256', '32768', '31889', 'new'),
           (8, '5555', '40000', '-719017797', 'new'),
           (9, '100', '5000', '5443', 'new'),
           (10, '17', '90000', '754072151', 'new');

explain
select insert_num,c1,c2,c3,b from table_change_numeric_group_string_group_floating_varchar_trunc order by insert_num;

select insert_num,c1,c2,c3,b from table_change_numeric_group_string_group_floating_varchar_trunc order by insert_num;

drop table table_change_numeric_group_string_group_floating_varchar_trunc;


------------------------------------------------------------------------------------------
-- SECTION: ALTER TABLE CHANGE COLUMNS for STRING_GROUP -> STRING_GROUP
--
--
--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for STRING_GROUP -> STRING_GROUP: STRING, (CHAR, CHAR trunc, VARCHAR, VARCHAR trunc)
--
CREATE TABLE table_change_string_group_string_group_string(insert_num int, c1 string, c2 string, c3 string, c4 string, b STRING);

insert into table table_change_string_group_string_group_string
    values (1, 'escapist', 'escapist', 'escapist', 'escapist', 'original'),
           (2, 'heartbeat', 'heartbeat', 'heartbeat', 'heartbeat', 'original'),
           (3, 'dynamic reptile', 'dynamic reptile', 'dynamic reptile', 'dynamic reptile', 'original'),
           (4, 'blank pads   ', 'blank pads   ', 'blank pads   ', 'blank pads   ', 'original');

select insert_num,c1,c2,c3,c4,b from table_change_string_group_string_group_string order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_string_group_string_group_string replace columns (insert_num int, c1 CHAR(50), c2 CHAR(9), c3 VARCHAR(50), c4 CHAR(9), b STRING) ;

insert into table table_change_string_group_string_group_string
    values (5, 'junkyard', 'junkyard', 'junkyard', 'junkyard', 'new'),
           (6, '  baffling    ', '  baffling    ', '  baffling    ', '  baffling    ', 'new'),
           (7, '           featherweight  ', '           featherweight  ','           featherweight  ','           featherweight  ', 'new'),
           (8, '  against', '  against', '  against', '  against', 'new'),
           (9, 'hangar paralysed companion ', 'hangar paralysed companion ', 'hangar paralysed companion ', 'hangar paralysed companion ', 'new'),
           (10, 'bottom  ', 'bottom  ', 'bottom  ', 'bottom  ', 'new');

explain
select insert_num,c1,c2,c3,c4,b from table_change_string_group_string_group_string order by insert_num;

select insert_num,c1,c2,c3,c4,b from table_change_string_group_string_group_string order by insert_num;

drop table table_change_string_group_string_group_string;



--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for STRING_GROUP -> STRING_GROUP: CHAR, (VARCHAR, VARCHAR trunc, STRING)
--
CREATE TABLE table_change_string_group_string_group_char(insert_num int, c1 CHAR(50), c2 CHAR(50), c3 CHAR(50), b STRING);

insert into table table_change_string_group_string_group_char
    values (1, 'escapist', 'escapist', 'escapist', 'original'),
           (2, 'heartbeat', 'heartbeat', 'heartbeat', 'original'),
           (3, 'dynamic reptile', 'dynamic reptile', 'dynamic reptile', 'original'),
           (4, 'blank pads   ', 'blank pads   ', 'blank pads   ', 'original');

select insert_num,c1,c2,c3,b from table_change_string_group_string_group_char order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_string_group_string_group_char replace columns (insert_num int, c1 VARCHAR(50), c2 VARCHAR(9), c3 STRING, b STRING) ;

insert into table table_change_string_group_string_group_char
    values (5, 'junkyard', 'junkyard', 'junkyard', 'new'),
           (6, '  baffling    ', '  baffling    ', '  baffling    ', 'new'),
           (7, '           featherweight  ', '           featherweight  ','           featherweight  ', 'new'),
           (8, '  against', '  against', '  against', 'new'),
           (9, 'hangar paralysed companion ', 'hangar paralysed companion ', 'hangar paralysed companion ', 'new'),
           (10, 'bottom  ', 'bottom  ', 'bottom  ', 'new');

explain
select insert_num,c1,c2,c3,b from table_change_string_group_string_group_char order by insert_num;

select insert_num,c1,c2,c3,b from table_change_string_group_string_group_char order by insert_num;

drop table table_change_string_group_string_group_char;



--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for STRING_GROUP -> STRING_GROUP: VARCHAR, (CHAR, CHAR trunc, STRING)
--
CREATE TABLE table_change_string_group_string_group_varchar(insert_num int, c1 VARCHAR(50), c2 VARCHAR(50), c3 VARCHAR(50), b STRING);

insert into table table_change_string_group_string_group_varchar
    values (1, 'escapist', 'escapist', 'escapist', 'original'),
           (2, 'heartbeat', 'heartbeat', 'heartbeat', 'original'),
           (3, 'dynamic reptile', 'dynamic reptile', 'dynamic reptile', 'original'),
           (4, 'blank pads   ', 'blank pads   ', 'blank pads   ', 'original');

select insert_num,c1,c2,c3,b from table_change_string_group_string_group_varchar order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_string_group_string_group_varchar replace columns (insert_num int, c1 CHAR(50), c2 CHAR(9), c3 STRING, b STRING) ;

insert into table table_change_string_group_string_group_varchar
    values (5, 'junkyard', 'junkyard', 'junkyard', 'new'),
           (6, '  baffling    ', '  baffling    ', '  baffling    ', 'new'),
           (7, '           featherweight  ', '           featherweight  ','           featherweight  ', 'new'),
           (8, '  against', '  against', '  against', 'new'),
           (9, 'hangar paralysed companion ', 'hangar paralysed companion ', 'hangar paralysed companion ', 'new'),
           (10, 'bottom  ', 'bottom  ', 'bottom  ', 'new');

explain
select insert_num,c1,c2,c3,b from table_change_string_group_string_group_varchar order by insert_num;

select insert_num,c1,c2,c3,b from table_change_string_group_string_group_varchar order by insert_num;

drop table table_change_string_group_string_group_varchar;



------------------------------------------------------------------------------------------
-- SECTION: ALTER TABLE CHANGE COLUMNS for "lower" type to "higher" NUMERIC_GROUP
--
--
--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for "lower" type to "higher" NUMERIC_GROUP: TINYINT, (SMALLINT, INT, BIGINT, DECIMAL, FLOAT, DOUBLE)
--
CREATE TABLE table_change_lower_to_higher_numeric_group_tinyint(insert_num int, c1 tinyint, c2 tinyint, c3 tinyint, c4 tinyint, c5 tinyint, c6 tinyint, b STRING);

insert into table table_change_lower_to_higher_numeric_group_tinyint
    values (1, 45, 45, 45, 45, 45, 45, 'original'),
           (2, -2, -2, -2, -2, -2, -2, 'original'),
           (3, -255, -255, -255, -255, -255, -255, 'original'),
           (4, 100, 100, 100, 100, 100, 100, 'original');

select insert_num,c1,c2,c3,c4,c5,c6,b from table_change_lower_to_higher_numeric_group_tinyint order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_lower_to_higher_numeric_group_tinyint replace columns (insert_num int, c1 SMALLINT, c2 INT, c3 BIGINT, c4 decimal(38,18), c5 FLOAT, c6 DOUBLE, b STRING) ;

insert into table table_change_lower_to_higher_numeric_group_tinyint
    values (5, '774', '2031', '200', '12', '99', '0', 'new'),
           (6, '561431', '52927714', '7203778961',  '8', '7', '6', 'new'),
           (7, '256', '32768', '31889', '300', '444', '506', 'new'),
           (8, '5555', '40000', '-719017797', '45', '55', '65', 'new'),
           (9, '100', '5000', '5443', '22', '2', '-2', 'new'),
           (10, '17', '90000', '754072151', '95', '20', '18', 'new');

explain
select insert_num,c1,c2,c3,c4,c5,c6,b from table_change_lower_to_higher_numeric_group_tinyint order by insert_num;

select insert_num,c1,c2,c3,c4,c5,c6,b from table_change_lower_to_higher_numeric_group_tinyint order by insert_num;

drop table table_change_lower_to_higher_numeric_group_tinyint;


--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for "lower" type to "higher" NUMERIC_GROUP: SMALLINT, (INT, BIGINT, DECIMAL, FLOAT, DOUBLE)
--
CREATE TABLE table_change_lower_to_higher_numeric_group_smallint(insert_num int, c1 smallint, c2 smallint, c3 smallint, c4 smallint, c5 smallint, b STRING);

insert into table table_change_lower_to_higher_numeric_group_smallint
    values (1, 2031, 2031, 2031, 2031, 2031, 'original'),
           (2, -2, -2, -2, -2, -2, 'original'),
           (3, -5000, -5000, -5000, -5000, -5000, 'original'),
           (4, 100, 100, 100, 100, 100, 'original');

select insert_num,c1,c2,c3,c4,c5,b from table_change_lower_to_higher_numeric_group_smallint order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_lower_to_higher_numeric_group_smallint replace columns (insert_num int, c1 INT, c2 BIGINT, c3 decimal(38,18), c4 FLOAT, c5 DOUBLE, b STRING) ;

insert into table table_change_lower_to_higher_numeric_group_smallint
    values (5, '774', '2031', '200', '12', '99', 'new'),
           (6, '561431', '52927714', '7203778961',  '8', '7', 'new'),
           (7, '256', '32768', '31889', '300', '444', 'new'),
           (8, '5555', '40000', '-719017797', '45', '55', 'new'),
           (9, '100', '5000', '5443', '22', '2', 'new'),
           (10, '17', '90000', '754072151', '95', '20', 'new');

explain
select insert_num,c1,c2,c3,c4,c5,b from table_change_lower_to_higher_numeric_group_smallint order by insert_num;

select insert_num,c1,c2,c3,c4,c5,b from table_change_lower_to_higher_numeric_group_smallint order by insert_num;

drop table table_change_lower_to_higher_numeric_group_smallint;



--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for "lower" type to "higher" NUMERIC_GROUP: INT, (BIGINT, DECIMAL, FLOAT, DOUBLE)
--
CREATE TABLE table_change_lower_to_higher_numeric_group_int(insert_num int, c1 int, c2 int, c3 int, c4 int, b STRING);

insert into table table_change_lower_to_higher_numeric_group_int
    values (1, 2031, 2031, 2031, 2031, 'original'),
           (2, -2, -2, -2, -2, 'original'),
           (3, -5000, -5000, -5000, -5000, 'original'),
           (4, 52927714, 52927714, 52927714, 52927714, 'original');

select insert_num,c1,c2,c3,c4,b from table_change_lower_to_higher_numeric_group_int order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_lower_to_higher_numeric_group_int replace columns (insert_num int, c1 BIGINT, c2 decimal(38,18), c3 FLOAT, c4 DOUBLE,  b STRING) ;

insert into table table_change_lower_to_higher_numeric_group_int
    values (5, '774', '2031', '200', '12', 'new'),
           (6, '561431', '52927714', '7203778961',  '8', 'new'),
           (7, '256', '32768', '31889', '300', 'new'),
           (8, '5555', '40000', '-719017797', '45', 'new'),
           (9, '100', '5000', '5443', '22', 'new'),
           (10, '17', '90000', '754072151', '95', 'new');

explain
select insert_num,c1,c2,c3,c4,b from table_change_lower_to_higher_numeric_group_int order by insert_num;

select insert_num,c1,c2,c3,c4,b from table_change_lower_to_higher_numeric_group_int order by insert_num;

drop table table_change_lower_to_higher_numeric_group_int;


--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for "lower" type to "higher" NUMERIC_GROUP: BIGINT, (DECIMAL, FLOAT, DOUBLE)
--
CREATE TABLE table_change_lower_to_higher_numeric_group_bigint(insert_num int, c1 bigint, c2 bigint, c3 bigint, b STRING);

insert into table table_change_lower_to_higher_numeric_group_bigint
    values (1, 7203778961, 7203778961, 7203778961, 'original'),
           (2, -2, -2, -2, 'original'),
           (3, -5000, -5000, -5000, 'original'),
           (4, 52927714, 52927714, 52927714, 'original');

select insert_num,c1,c2,c3,b from table_change_lower_to_higher_numeric_group_bigint order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_lower_to_higher_numeric_group_bigint replace columns (insert_num int, c1 decimal(38,18), c2 FLOAT, c3 DOUBLE, b STRING) ;

insert into table table_change_lower_to_higher_numeric_group_bigint
    values (5, '774', '2031', '200', 'new'),
           (6, '561431', '52927714', '7203778961', 'new'),
           (7, '256', '32768', '31889', 'new'),
           (8, '5555', '40000', '-719017797', 'new'),
           (9, '100', '5000', '5443', 'new'),
           (10, '17', '90000', '754072151', 'new');

explain
select insert_num,c1,c2,c3,b from table_change_lower_to_higher_numeric_group_bigint order by insert_num;

select insert_num,c1,c2,c3,b from table_change_lower_to_higher_numeric_group_bigint order by insert_num;

drop table table_change_lower_to_higher_numeric_group_bigint;


--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for "lower" type to "higher" NUMERIC_GROUP: FLOAT, (DOUBLE, DECIMAL)
--
CREATE TABLE table_change_lower_to_higher_numeric_group_decimal(insert_num int, c1 float, c2 float, b STRING);

insert into table table_change_lower_to_higher_numeric_group_decimal
    values (1, -29.0764, -29.0764, 'original'),
           (2, 753.7028, 753.7028, 'original'),
           (3, -5000, -5000, 'original'),
           (4, 52927714, 52927714, 'original');

select insert_num,c1,c2,b from table_change_lower_to_higher_numeric_group_decimal order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_lower_to_higher_numeric_group_decimal replace columns (insert_num int, c1 double, c2 decimal(38,18), b STRING) ;

insert into table table_change_lower_to_higher_numeric_group_decimal
    values (5, '7.74', '22.3', 'new'),
           (6, '56.1431', '90.9', 'new'),
           (7, '2.56', '25.6', 'new'),
           (8, '555.5', '55.55', 'new'),
           (9, '10.0', '0.100', 'new'),
           (10, '1.7', '17.8888', 'new');

explain
select insert_num,c1,c2,b from table_change_lower_to_higher_numeric_group_decimal order by insert_num;

select insert_num,c1,c2,b from table_change_lower_to_higher_numeric_group_decimal order by insert_num;

drop table table_change_lower_to_higher_numeric_group_decimal;



--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for "lower" type to "higher" NUMERIC_GROUP: DOUBLE, (DECIMAL)
--
CREATE TABLE table_change_lower_to_higher_numeric_group_float(insert_num int, c1 double, b STRING);

insert into table table_change_lower_to_higher_numeric_group_float
    values (1, -29.0764, 'original'),
           (2, 753.7028, 'original'),
           (3, -5000, 'original'),
           (4, 52927714, 'original');

select insert_num,c1,b from table_change_lower_to_higher_numeric_group_float order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table_change_lower_to_higher_numeric_group_float replace columns (insert_num int, c1 decimal(38,18), b STRING) ;

insert into table table_change_lower_to_higher_numeric_group_float
    values (5, '774', 'new'),
           (6, '561431', 'new'),
           (7, '256', 'new'),
           (8, '5555', 'new'),
           (9, '100', 'new'),
           (10, '17', 'new');

explain
select insert_num,c1,b from table_change_lower_to_higher_numeric_group_float order by insert_num;

select insert_num,c1,b from table_change_lower_to_higher_numeric_group_float order by insert_num;

drop table table_change_lower_to_higher_numeric_group_float;