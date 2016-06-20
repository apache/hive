set hive.explain.user=true;
set hive.mapred.mode=nonstrict;
set hive.cli.print.header=true;
SET hive.exec.schema.evolution=true;
SET hive.vectorized.use.vectorized.input.format=true;
SET hive.vectorized.use.vector.serde.deserialize=false;
SET hive.vectorized.use.row.serde.deserialize=false;
set hive.fetch.task.conversion=none;
SET hive.vectorized.execution.enabled=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.metastore.disallow.incompatible.col.type.changes=false;
set hive.default.fileformat=orc;

-- SORT_QUERY_RESULTS
--
-- FILE VARIATION: ORC, Vectorized, MapWork, Partitioned --> all primitive conversions
--
------------------------------------------------------------------------------------------
-- SECTION: ALTER TABLE CHANGE COLUMNS Various --> Various
--
--
--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for Various --> Various: (BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DECIMAL, STRING, TIMESTAMP) --> BOOLEAN
--
CREATE TABLE part_change_various_various_boolean(insert_num int, c1 TINYINT, c2 SMALLINT, c3 INT, c4 BIGINT, c5 FLOAT, c6 DOUBLE, c7 DECIMAL(38,18), c8 STRING, c9 TIMESTAMP, b STRING) PARTITIONED BY(part INT);

insert into table part_change_various_various_boolean partition(part=1)
    values(1, 255, 2000, 72909, 3244222, -29.0764, 470614135, 470614135, 'true', '0004-09-22 18:26:29.519542222', 'original'),
          (2, 45, 1000, 483777, -23866739993, -3651.672121, 46114.284799488, 46114.284799488, '', '2007-02-09 05:17:29.368756876', 'original'),
          (3, 200, 72909, 3244222, -93222, 30.774, -66475.561431, -66475.561431, '1', '6229-06-28 02:54:28.970117179', 'original'),
          (4, 1, 90000, 754072151, 3289094, 46114.284799488 ,9250340.75, 9250340.75, 'time will come', '2002-05-10 05:29:48.990818073', 'original');

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,b from part_change_various_various_boolean order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table part_change_various_various_boolean replace columns (insert_num int, c1 BOOLEAN, c2 BOOLEAN, c3 BOOLEAN, c4 BOOLEAN, c5 BOOLEAN, c6 BOOLEAN, c7 BOOLEAN, c8 BOOLEAN, c9 BOOLEAN, b STRING);

insert into table part_change_various_various_boolean partition(part=2)
    values (5, 1, true, false, 1, 0, false, false, true, false, 'new');

insert into table part_change_various_various_boolean partition(part=1)
    values (6, 0, 1, 1, false, 0, true, false, true, 0, 'new');

explain
select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,b from part_change_various_various_boolean order by insert_num;

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,b from part_change_various_various_boolean order by insert_num;

drop table part_change_various_various_boolean;



--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for Various --> Various: (BOOLEAN, SHORT, INT, LONG, FLOAT, DOUBLE, DECIMAL, STRING, CHAR, VARCHAR, TIMESTAMP) --> BYTE
-- -128 and a maximum value of 127
--
CREATE TABLE part_change_various_various_tinyint(insert_num int, c1 BOOLEAN, c2 SMALLINT, c3 INT, c4 BIGINT, c5 FLOAT, c6 DOUBLE, c7 DECIMAL(38,18), c8 STRING, c9 CHAR(25), c10 VARCHAR(25), c11 TIMESTAMP, b STRING) PARTITIONED BY(part INT);

insert into table part_change_various_various_tinyint partition(part=1)
    values(1, true, 2000, 72909, 3244222, -29.0764, 470614135, 470614135, '129', '-128', '-2999', '0004-09-22 18:26:29.519542222', 'original'),
          (2, 0, -128, -48, -20, -9223372036854775808.0, -9223372036854775808.0, 9223372036854775807.0, '128', '-99', '40', '2007-02-09 05:17:29.368756876', 'original'),
          (3, -1, -129, 100, 499, -9223372036854775809.0, -9223372036854775809.0, 9223372036854775808.0, '128', '-99', '40', '2007-02-09 05:17:29.368756876', 'original'),
          (4, false, -72, -127, 127, 30.774, 127.561431, -106.561431, '90.284799488', '90.284799488', '1', '6229-06-28 02:54:28.970117179', 'original'),
          (5, 1, -90, 75, -38, 109.284799488 ,-128.75, 98.75, '120.4', '33.333', '0.45', '2002-05-10 05:29:48.990818073', 'original');

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,b from part_change_various_various_tinyint order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table part_change_various_various_tinyint replace columns (insert_num int, c1 TINYINT, c2 TINYINT, c3 TINYINT, c4 TINYINT, c5 TINYINT, c6 TINYINT, c7 TINYINT, c8 TINYINT, c9 TINYINT, c10 TINYINT, c11 TINYINT, b STRING);

insert into table part_change_various_various_tinyint partition(part=2)
    values (6, 23, 71, 127, 1, 131, -60, 68, -230, -182, 40, 93, 'new');

insert into table part_change_various_various_tinyint partition(part=1)
    values (7, -120, 85, -126, -167, 91, 113, -28, -63, 0, 8, 237, 'new');

explain
select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,b from part_change_various_various_tinyint order by insert_num;

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,b from part_change_various_various_tinyint order by insert_num;

drop table part_change_various_various_tinyint;



--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for Various --> Various: (BOOLEAN, TINYINT, INT, LONG, FLOAT, DOUBLE, DECIMAL, STRING, CHAR, VARCHAR, TIMESTAMP) --> SMALLINT
-- -32768 and a maximum value of 32767 
--
CREATE TABLE part_change_various_various_smallint(insert_num int, c1 BOOLEAN, c2 TINYINT, c3 INT, c4 BIGINT, c5 FLOAT, c6 DOUBLE, c7 DECIMAL(38,18), c8 STRING, c9 CHAR(25), c10 VARCHAR(25), c11 TIMESTAMP, b STRING) PARTITIONED BY(part INT);

insert into table part_change_various_various_smallint partition(part=1)
    values(1, -2999, 200, 72909, 3244222, -29.0764, 470614135, 470614135, '-2999', '-2999', '-2999', '0004-09-22 18:26:29.519542222', 'original'),
          (2, 0, 100, -32768 , 32767, -3651.672121, -9223372036854775808.0, 9223372036854775807.0, '9000', '32767', '-32768', '2007-02-09 05:17:29.368756876', 'original'),
          (3, 0, -127, -40000 , 32768, -3651.672121, -9223372036854775809.0, 9223372036854775808.0, '9000', '32767', '-32768', '2007-02-09 05:17:29.368756876', 'original'),
          (4, false, 72, 32422, -9322, 30.774, -6675.561431, -6675.561431, '1', '1', '1', '6229-06-28 02:54:28.970117179', 'original'),
          (5, 1, -90, 7151, 3094, 30000.284799488 ,-9000.75, 0.75, '5299', '5299', '5299', '2002-05-10 05:29:48.990818073', 'original');

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,b from part_change_various_various_smallint order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table part_change_various_various_smallint replace columns (insert_num int, c1 SMALLINT, c2 SMALLINT, c3 SMALLINT, c4 SMALLINT, c5 SMALLINT, c6 SMALLINT, c7 SMALLINT, c8 SMALLINT, c9 SMALLINT, c10 SMALLINT, c11 SMALLINT, b STRING);

insert into table part_change_various_various_smallint partition(part=2)
    values (6, -30486, 15230, 3117, 1, -117, -7131, 20227, -24858, -28771, 46114, 72909, 'new');

insert into table part_change_various_various_smallint partition(part=1)
    values (7, -10542, -1805, -4844, 15507, 91, 22385, -28, -12268, 0, 66475, 774, 'new');

explain
select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,b from part_change_various_various_smallint order by insert_num;

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,b from part_change_various_various_smallint order by insert_num;

drop table part_change_various_various_smallint;


--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for Various --> Various: (BOOLEAN, TINYINT, SMALLINT, LONG, FLOAT, DOUBLE, DECIMAL, STRING, CHAR, VARCHAR, TIMESTAMP) --> INT
-- –2147483648 to 2147483647
--
CREATE TABLE part_change_various_various_int(insert_num int, c1 BOOLEAN, c2 TINYINT, c3 SMALLINT, c4 BIGINT, c5 FLOAT, c6 DOUBLE, c7 DECIMAL(38,18), c8 STRING, c9 CHAR(25), c10 VARCHAR(25), c11 TIMESTAMP, b STRING) PARTITIONED BY(part INT);

insert into table part_change_various_various_int partition(part=1)
    values(1, -2999, 200, 72909, 3244222, -29.0764, 470614135, 470614135, '-2999', '-2999', '-2999', '0004-09-22 18:26:29.519542222', 'original'),
          (2, 0, 100, 2147483647, -23866739993, -3651.672121, -9223372036854775808.0, 9223372036854775807.0, '', '', '', '2007-02-09 05:17:29.368756876', 'original'),
          (3, 0, 100, 2147483648, -23866739993, -3651.672121, -9223372036854775809.0, 9223372036854775808.0, '', '', '', '2007-02-09 05:17:29.368756876', 'original'),
          (4, false, 72, 3244222, -93222, 30.774, -66475.561431, -66475.561431, '1', '1', '1', '6229-06-28 02:54:28.970117179', 'original'),
          (5, 1, -90, 754072151, 3289094, 46114.284799488 ,9250340.75, 9250340.75, '5299', '5299', '5299', '2002-05-10 05:29:48.990818073', 'original');

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,b from part_change_various_various_int order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table part_change_various_various_int replace columns (insert_num int, c1 INT, c2 INT, c3 INT, c4 INT, c5 INT, c6 INT, c7 INT, c8 INT, c9 INT, c10 INT, c11 INT, b STRING);

insert into table part_change_various_various_int partition(part=2)
    values (5, 560930, -1281818, 127, 1, 84269672, -60, 27094665, -36016110, -182, 3244222, 561431, 'new');

insert into table part_change_various_various_int partition(part=1)
    values (6, -1928921, 695025, -151775655, -167, 91, 113, -164341325, -134237413, 0, 6229, 4422, 'new');

explain
select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,b from part_change_various_various_int order by insert_num;

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,b from part_change_various_various_int order by insert_num;

drop table part_change_various_various_int;



--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for Various --> Various: (BOOLEAN, TINYINT, SMALLINT, INT, FLOAT, DOUBLE, DECIMAL, STRING, CHAR, VARCHAR, TIMESTAMP) --> BIGINT
-- -9223372036854775808 to 9223372036854775807
--
CREATE TABLE part_change_various_various_bigint(insert_num int, c1 BOOLEAN, c2 TINYINT, c3 SMALLINT, c4 INT, c5 FLOAT, c6 DOUBLE, c7 DECIMAL(38,18), c8 STRING, c9 CHAR(25), c10 VARCHAR(25), c11 TIMESTAMP, b STRING) PARTITIONED BY(part INT);

insert into table part_change_various_various_bigint partition(part=1)
    values(1, -2999, 200, 72909, 3244222, -29.0764, 470614135, 470614135, '-2999', '-2999', '-2999', '0004-09-22 18:26:29.519542222', 'original'),
          (2, 0, 100, 32767, -23372036854775, -3651.672121, -9223372036854775808.0, 9223372036854775807.0, '', '', '', '2007-02-09 05:17:29.368756876', 'original'),
          (3, 0, 100, -32768, 23372036854775, -3651.672121, -9223372036854775809.0, 9223372036854775808.0, '', '', '', '2007-02-09 05:17:29.368756876', 'original'),
          (4, false, 72, 3244222, -93222, 30.774, -66475.561431, -66475.561431, '1', '1', '1', '6229-06-28 02:54:28.970117179', 'original'),
          (5, 1, -90, 754072151, 3289094, 46114.284799488 ,9250340.75, 9250340.75, '1998287.3541', '1998287.3541', '1998287.3541', '2002-05-10 05:29:48.990818073', 'original');

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,b from part_change_various_various_bigint order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table part_change_various_various_bigint replace columns (insert_num int, c1 BIGINT, c2 BIGINT, c3 BIGINT, c4 BIGINT, c5 BIGINT, c6 BIGINT, c7 BIGINT, c8 BIGINT, c9 BIGINT, c10 BIGINT, c11 BIGINT, b STRING);

insert into table part_change_various_various_bigint partition(part=2)
    values (6, 5573199346255528403, 71, 151775655, 1, 131, -60, 6275638713485623898, -230, -695025, 519542222, -29.0764, 'new');

insert into table part_change_various_various_bigint partition(part=1)
    values (7, -164341325, 9043162437544575070, -126, -6566204574741299000, 91, 113, -28, -63, 0, 3244222, -90, 'new');

explain
select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,b from part_change_various_various_bigint order by insert_num;

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,b from part_change_various_various_bigint order by insert_num;

drop table part_change_various_various_bigint;



--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for Various --> Various: (BOOLEAN, TINYINT, SMALLINT, INT, LONG, DOUBLE, DECIMAL, STRING, CHAR, VARCHAR, TIMESTAMP) --> FLOAT
--
CREATE TABLE part_change_various_various_float(insert_num int, c1 BOOLEAN, c2 TINYINT, c3 SMALLINT, c4 INT, c5 BIGINT, c6 DOUBLE, c7 DECIMAL(38,18), c8 STRING, c9 CHAR(25), c10 VARCHAR(25), c11 TIMESTAMP, b STRING) PARTITIONED BY(part INT);

insert into table part_change_various_various_float partition(part=1)
    values(1, -2999, 200, 72909, 3244222, -29.0764, 470614135, 470614135, '-2999', '-2999', '-2999', '0004-09-22 18:26:29.519542222', 'original'),
          (2, 0, 100, 32767, -23372036854775, -3651.672121, -9223372036854775808.0, 9223372036854775807.0, '', '', '', '2007-02-09 05:17:29.368756876', 'original'),
          (3, 0, 100, -32768, 23372036854775, -3651.672121, -9223372036854775809.0, 9223372036854775808.0, '', '', '', '2007-02-09 05:17:29.368756876', 'original'),
          (4, false, 72, 3244222, -93222, 30.774, -66475.561431, -66475.561431, '2402.3', '2402.3', '2402.3', '6229-06-28 02:54:28.970117179', 'original'),
          (5, 1, -90, 754072151, 3289094, 46114.284799488 ,9250340.75, 9250340.75, '5299', '5299', '5299', '2002-05-10 05:29:48.990818073', 'original');

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,b from part_change_various_various_float order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table part_change_various_various_float replace columns (insert_num int, c1 FLOAT, c2 FLOAT, c3 FLOAT, c4 FLOAT, c5 FLOAT, c6 FLOAT, c7 FLOAT, c8 FLOAT, c9 FLOAT, c10 FLOAT, c11 FLOAT, b STRING);

insert into table part_change_various_various_float partition(part=2)
    values (6, 953967041., 62.0791539559013466, 718.78, 1, 203.199548118, -60, 6275638713485623898, -230, -695025, -3651.67212, 46114.28, 'new');

insert into table part_change_various_various_float partition(part=1)
    values (7, -1255178165.77663, 9043162437544575070.974, -4314.7918, -1240033819, 91, 1698.95, -100.3597812, -63, 0, -93222.200, 29.076, 'new');

explain
select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,b from part_change_various_various_float order by insert_num;

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,b from part_change_various_various_float order by insert_num;

drop table part_change_various_various_float;



--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for Various --> Various: (BOOLEAN, TINYINT, SMALLINT, INT, LONG, FLOAT, DECIMAL, STRING, CHAR, VARCHAR, TIMESTAMP) --> DOUBLE
--
CREATE TABLE part_change_various_various_double(insert_num int, c1 BOOLEAN, c2 TINYINT, c3 SMALLINT, c4 INT, c5 BIGINT, c6 DOUBLE, c7 DECIMAL(38,18), c8 STRING, c9 CHAR(25), c10 VARCHAR(25), c11 TIMESTAMP, b STRING) PARTITIONED BY(part INT);

insert into table part_change_various_various_double partition(part=1)
    values(1, -2999, 200, 72909, 3244222, -29.0764, 470614135, 470614135, '-2999', '-2999', '-2999', '0004-09-22 18:26:29.519542222', 'original'),
          (2, 0, 100, 32767, -23372036854775, -3651.672121, -9223372036854775808.0, 9223372036854775807.0, '', '', '', '2007-02-09 05:17:29.368756876', 'original'),
          (3, 0, 100, -32768, 23372036854775, -3651.672121, -9223372036854775809.0, 9223372036854775808.0, '', '', '', '2007-02-09 05:17:29.368756876', 'original'),
          (4, false, 72, 3244222, -93222, 30.774, -66475.561431, -66475.561431, '1', '1', '1', '6229-06-28 02:54:28.970117179', 'original'),
          (5, 1, -90, 754072151, 3289094, 46114.284799488 ,9250340.75, 9250340.75, '5299', '5299', '5299', '2002-05-10 05:29:48.990818073', 'original');

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,b from part_change_various_various_double order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table part_change_various_various_double replace columns (insert_num int, c1 DOUBLE, c2 DOUBLE, c3 DOUBLE, c4 DOUBLE, c5 DOUBLE, c6 DOUBLE, c7 DOUBLE, c8 DOUBLE, c9 DOUBLE, c10 DOUBLE, c11 DOUBLE, b STRING);

insert into table part_change_various_various_double partition(part=2)
    values (6, 953967041., 62.0791539559013466, 718.78, 1, 203.199548118, -60, 6275638713485623898, -230, -695025, 0.00007011717, 4.28479948, 'new');

insert into table part_change_various_various_double partition(part=1)
    values (7, -1255178165.77663, 9043162437544575070.974, -4314.7918, -1240033819, 91, 1698.95, -100.3597812, -63, 0, -66475.0000008, -284799488.1, 'new');

explain
select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,b from part_change_various_various_double order by insert_num;

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,b from part_change_various_various_double order by insert_num;

drop table part_change_various_various_double;



--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for Various --> Various: (BOOLEAN, TINYINT, SMALLINT, INT, LONG, FLOAT, DOUBLE, STRING, CHAR, VARCHAR, TIMESTAMP) --> DECIMAL
--
CREATE TABLE part_change_various_various_decimal(insert_num int, c1 BOOLEAN, c2 TINYINT, c3 SMALLINT, c4 INT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 STRING, c9 CHAR(25), c10 VARCHAR(25), c11 TIMESTAMP, b STRING) PARTITIONED BY(part INT);

insert into table part_change_various_various_decimal partition(part=1)
    values(1, -2999, 200, 72909, 3244222, -29.0764, 470614135, 470614135, '--1551801.09502', '--1551801.09502', '--1551801.09502', '0004-09-22 18:26:29.519542222', 'original'),
          (2, 0, 100, 32767, -23372036854775, -3651.672121, -9223372036854775808.0, 9223372036854775807.0, '', '', '', '2007-02-09 05:17:29.368756876', 'original'),
          (3, 0, 100, -32768, 23372036854775, -3651.672121, -9223372036854775809.0, 9223372036854775808.0, '', '', '', '2007-02-09 05:17:29.368756876', 'original'),
          (4, false, 72, 3244222, -93222, 30.774, -66475.561431, -66475.561431, '1', '1', '1', '6229-06-28 02:54:28.970117179', 'original'),
          (5, 1, -90, 754072151, 3289094, 46114.284799488 ,9250340.75, 9250340.75, '2402.3', '2402.3', '2402.3', '2002-05-10 05:29:48.990818073', 'original');

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,b from part_change_various_various_decimal order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table part_change_various_various_decimal replace columns (insert_num int, c1 DECIMAL(38,18), c2 DECIMAL(38,18), c3 DECIMAL(38,18), c4 DECIMAL(38,18), c5 DECIMAL(38,18), c6 DECIMAL(38,18), c7 DECIMAL(38,18), c8 DECIMAL(38,18), c9 DECIMAL(38,18), c10 DECIMAL(38,18), c11 DECIMAL(38,18), b STRING);

insert into table part_change_various_various_decimal partition(part=2)
    values (6, 953967041., 62.0791539559013466, 718.78, 1, 203.199548118, -60, 6275638713485623898, -230, -695025, 0.00007011717, 4.28479948, 'new');

insert into table part_change_various_various_decimal partition(part=1)
    values (7,-1255178165.77663, 9043162437544575070.974, -4314.7918, -1240033819, 91, 1698.95, -100.3597812, -63, 0, -66475.0000008, -284799488.1, 'new');

explain
select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,b from part_change_various_various_decimal order by insert_num;

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,b from part_change_various_various_decimal order by insert_num;

drop table part_change_various_various_decimal;



--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for Various --> Various: (BOOLEAN, TINYINT, SMALLINT, INT, LONG, FLOAT, DOUBLE, DECIMAL(38,18), CHAR, VARCHAR, TIMESTAMP, DATE, BINARY) --> STRING
--
CREATE TABLE part_change_various_various_string(insert_num int, c1 BOOLEAN, c2 TINYINT, c3 SMALLINT, c4 INT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 DECIMAL(38,18), c9 CHAR(25), c10 VARCHAR(25), c11 TIMESTAMP, c12 DATE, c13 BINARY, b STRING) PARTITIONED BY(part INT);

insert into table part_change_various_various_string partition(part=1)
    values(1, true,  200,  72909,      3244222, -99999999999,     -29.0764,      470614135,        470614135,         'dynamic reptile  ', 'dynamic reptile  ',  '0004-09-22 18:26:29.519542222', '2007-02-09', 'binary', 'original'),
          (2, 0, 100, 32767, -23372036854775, -3651.672121, -9223372036854775808.0, 9223372036854775807.0, '', '', '', '2007-02-09 05:17:29.368756876', 'original'),
          (3, 0, 100, -32768, 23372036854775, -3651.672121, -9223372036854775809.0, 9223372036854775808.0, '', '', '', '2007-02-09 05:17:29.368756876', 'original'),
          (4, false, 72,   3244222,    -93222,   30.774,       -   66475.561431, -66475.561431,    0.561431,          '1', '1',                                  '6229-06-28 02:54:28.970117179', '5966-07-09', 'binary', 'original'),
          (5, 1,    -90,   754072151,   3289094, 46114.284799488,  9250340.75,    9250340.75,      9250340.75,        'junkyard', 'junkyard',                    '2002-05-10 05:29:48.990818073', '1815-05-06', 'binary', 'original');

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,b from part_change_various_various_string order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table part_change_various_various_string replace columns (insert_num int, c1 STRING, c2 STRING, c3 STRING, c4 STRING, c5 STRING, c6 STRING, c7 STRING, c8 STRING, c9 STRING, c10 STRING, c11 STRING, c12 STRING, c13 STRING, b STRING);

insert into table part_change_various_various_string partition(part=2)
    values (6, 'true', '400',  '44388',       -'100',    '953967041.',       '62.079153',     '718.78',         '1',                'verdict', 'verdict', 'timestamp', 'date', 'binary', 'new');

insert into table part_change_various_various_string partition(part=1)
    values (7,-'false', '-67', '833',          '63993', ' 1255178165.77663', '905070.974', '-4314.7918',        -'1240033819',      'trial',   'trial',  '2016-03-07 03:02:22.0', '2016-03-07', 'binary', 'new');

explain
select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,b from part_change_various_various_string order by insert_num;

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,b from part_change_various_various_string order by insert_num;

drop table part_change_various_various_string;



--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for Various --> Various: (BOOLEAN, TINYINT, SMALLINT, INT, LONG, FLOAT, DOUBLE, DECIMAL(38,18), STRING, VARCHAR, TIMESTAMP, DATE, BINARY) --> CHAR
--
CREATE TABLE part_change_various_various_char(insert_num int, c1 BOOLEAN, c2 TINYINT, c3 SMALLINT, c4 INT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 DECIMAL(38,18), c9 STRING, c10 VARCHAR(25), c11 TIMESTAMP, c12 DATE, c13 BINARY, b STRING) PARTITIONED BY(part INT);

insert into table part_change_various_various_char partition(part=1)
    values(1, true,  200,  72909,      3244222, -99999999999,     -29.0764,      470614135,        470614135,         'dynamic reptile  ', 'dynamic reptile  ',  '0004-09-22 18:26:29.519542222', '2007-02-09', 'binary', 'original'),
          (2, 0, 100, 32767, -23372036854775, -3651.672121, -9223372036854775808.0, 9223372036854775807.0, '', '', '', '2007-02-09 05:17:29.368756876', 'original'),
          (3, 0, 100, -32768, 23372036854775, -3651.672121, -9223372036854775809.0, 9223372036854775808.0, '', '', '', '2007-02-09 05:17:29.368756876', 'original'),
          (4, false, 72,   3244222,    -93222,   30.774,       -   66475.561431, -66475.561431,    0.561431,         '1', '1',                                  '6229-06-28 02:54:28.970117179', '5966-07-09', 'binary', 'original'),
          (5, 1,    -90,   754072151,   3289094, 46114.284799488,  9250340.75,    9250340.75,      9250340.75,        'junkyard', 'junkyard',                    '2002-05-10 05:29:48.990818073', '1815-05-06', 'binary', 'original');

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,b from part_change_various_various_char order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table part_change_various_various_char replace columns (insert_num int, c1 CHAR(25), c2 CHAR(25), c3 CHAR(25), c4 CHAR(25), c5 CHAR(25), c6 CHAR(25), c7 CHAR(25), c8 CHAR(25), c9 CHAR(25), c10 CHAR(25), c11 CHAR(25), c12 CHAR(25), c13 CHAR(25), b STRING);

insert into table part_change_various_various_char partition(part=2)
    values (6, 'true', '400',  '44388',       -'100',    '953967041.',       '62.079153',     '718.78',         '1',                'verdict', 'verdict', 'timestamp', 'date', 'binary', 'new');

insert into table part_change_various_various_char partition(part=1)
    values (7,-'false', '-67', '833',          '63993', ' 1255178165.77663', '905070.974', '-4314.7918',        -'1240033819',      'trial',   'trial',  '2016-03-07 03:02:22.0', '2016-03-07', 'binary', 'new');

explain
select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,b from part_change_various_various_char order by insert_num;

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,b from part_change_various_various_char order by insert_num;

drop table part_change_various_various_char;



--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for Various --> Various: (BOOLEAN, TINYINT, SMALLINT, INT, LONG, FLOAT, DOUBLE, DECIMAL(38,18), STRING, VARCHAR, TIMESTAMP, DATE, BINARY) --> CHAR trunc
--
CREATE TABLE part_change_various_various_char_trunc(insert_num int, c1 BOOLEAN, c2 TINYINT, c3 SMALLINT, c4 INT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 DECIMAL(38,18), c9 STRING, c10 VARCHAR(8), c11 TIMESTAMP, c12 DATE, c13 BINARY, b STRING) PARTITIONED BY(part INT);

insert into table part_change_various_various_char_trunc partition(part=1)
    values(1, true,  200,  72909,      3244222, -99999999999,     -29.0764,      470614135,        470614135,         'dynamic reptile  ', 'dynamic reptile  ',  '0004-09-22 18:26:29.519542222', '2007-02-09', 'binary', 'original'),
          (2, 0, 100, 32767, -23372036854775, -3651.672121, -9223372036854775808.0, 9223372036854775807.0, '', '', '', '2007-02-09 05:17:29.368756876', 'original'),
          (3, 0, 100, -32768, 23372036854775, -3651.672121, -9223372036854775809.0, 9223372036854775808.0, '', '', '', '2007-02-09 05:17:29.368756876', 'original'),
          (4, false, 72,   3244222,    -93222,   30.774,       -   66475.561431, -66475.561431,    0.561431,         '1', '1',                                  '6229-06-28 02:54:28.970117179', '5966-07-09', 'binary', 'original'),
          (5, 1,    -90,   754072151,   3289094, 46114.284799488,  9250340.75,    9250340.75,      9250340.75,        'junkyard', 'junkyard',                    '2002-05-10 05:29:48.990818073', '1815-05-06', 'binary', 'original');

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,b from part_change_various_various_char_trunc order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table part_change_various_various_char_trunc replace columns (insert_num int, c1 CHAR(8), c2 CHAR(8), c3 CHAR(8), c4 CHAR(8), c5 CHAR(8), c6 CHAR(8), c7 CHAR(8), c8 CHAR(8), c9 CHAR(8), c10 CHAR(8), c11 CHAR(8), c12 CHAR(8), c13 CHAR(8), b STRING);

insert into table part_change_various_various_char_trunc partition(part=2)
    values (6, 'true', '400',  '44388',       -'100',    '953967041.',       '62.079153',     '718.78',         '1',                'verdict', 'verdict', 'timestamp', 'date', 'binary', 'new');

insert into table part_change_various_various_char_trunc partition(part=1)
    values (7,-'false', '-67', '833',          '63993', ' 1255178165.77663', '905070.974', '-4314.7918',        -'1240033819',      'trial',   'trial',  '2016-03-07 03:02:22.0', '2016-03-07', 'binary', 'new');

explain
select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,b from part_change_various_various_char_trunc order by insert_num;

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,b from part_change_various_various_char_trunc order by insert_num;

drop table part_change_various_various_char_trunc;



--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for Various --> Various: (BOOLEAN, TINYINT, SMALLINT, INT, LONG, FLOAT, DOUBLE, DECIMAL(38,18), STRING, CHAR, TIMESTAMP, DATE, BINARY) --> VARCHAR
--
CREATE TABLE part_change_various_various_varchar(insert_num int, c1 BOOLEAN, c2 TINYINT, c3 SMALLINT, c4 INT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 DECIMAL(38,18), c9 STRING, c10 CHAR(25), c11 TIMESTAMP, c12 DATE, c13 BINARY, b STRING) PARTITIONED BY(part INT);

insert into table part_change_various_various_varchar partition(part=1)
    values(1, true,  200,  72909,      3244222, -99999999999,     -29.0764,      470614135,        470614135,         'dynamic reptile  ', 'dynamic reptile  ',  '0004-09-22 18:26:29.519542222', '2007-02-09', 'binary', 'original'),
          (2, 0, 100, 32767, -23372036854775, -9223372036854775808.0, -9223372036854775808.0, 9223372036854775807.0, '', '', '', '2007-02-09 05:17:29.368756876', 'original'),
          (3, 0, 100, -32768, 23372036854775, -9223372036854775809.0, -9223372036854775809.0, 9223372036854775808.0, '', '', '', '2007-02-09 05:17:29.368756876', 'original'),
          (4, false, 72,   3244222,    -93222,   30.774,       -   66475.561431, -66475.561431,    0.561431,         '1', '1',                                  '6229-06-28 02:54:28.970117179', '5966-07-09', 'binary', 'original'),
          (5, 1,    -90,   754072151,   3289094, 46114.284799488,  9250340.75,    9250340.75,      9250340.75,        'junkyard', 'junkyard',                    '2002-05-10 05:29:48.990818073', '1815-05-06', 'binary', 'original');

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,b from part_change_various_various_varchar order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table part_change_various_various_varchar replace columns (insert_num int, c1 VARCHAR(25), c2 VARCHAR(25), c3 VARCHAR(25), c4 VARCHAR(25), c5 VARCHAR(25), c6 VARCHAR(25), c7 VARCHAR(25), c8 VARCHAR(25), c9 VARCHAR(25), c10 VARCHAR(25), c11 VARCHAR(25), c12 VARCHAR(25), c13 VARCHAR(25), b STRING);

insert into table part_change_various_various_varchar partition(part=2)
    values (6, 'true', '400',  '44388',       -'100',    '953967041.',       '62.079153',     '718.78',         '1',                'verdict', 'verdict', 'timestamp', 'date', 'binary', 'new');

insert into table part_change_various_various_varchar partition(part=1)
    values (7,-'false', '-67', '833',          '63993', ' 1255178165.77663', '905070.974', '-4314.7918',        -'1240033819',      'trial',   'trial',  '2016-03-07 03:02:22.0', '2016-03-07', 'binary', 'new');

explain
select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,b from part_change_various_various_varchar order by insert_num;

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,b from part_change_various_various_varchar order by insert_num;

drop table part_change_various_various_varchar;



--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for Various --> Various: (BOOLEAN, TINYINT, SMALLINT, INT, LONG, FLOAT, DOUBLE, DECIMAL(38,18), STRING, CHAR, TIMESTAMP, DATE, BINARY) --> VARCHAR trunc
--
CREATE TABLE part_change_various_various_varchar_trunc(insert_num int, c1 BOOLEAN, c2 TINYINT, c3 SMALLINT, c4 INT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 DECIMAL(38,18), c9 STRING, c10 CHAR(25), c11 TIMESTAMP, c12 DATE, c13 BINARY, b STRING) PARTITIONED BY(part INT);

insert into table part_change_various_various_varchar_trunc partition(part=1)
    values(1, true,  200,  72909,      3244222, -99999999999,     -29.0764,      470614135,        470614135,         'dynamic reptile  ', 'dynamic reptile  ',  '0004-09-22 18:26:29.519542222', '2007-02-09', 'binary', 'original'),
          (2, 0, 100, 32767, -23372036854775, -9223372036854775808.0, -9223372036854775808.0, 9223372036854775807.0, '', '', '', '2007-02-09 05:17:29.368756876', 'original'),
          (3, 0, 100, -32768, 23372036854775, -9223372036854775809.0, -9223372036854775809.0, 9223372036854775808.0, '', '', '', '2007-02-09 05:17:29.368756876', 'original'),
          (3, false, 72,   3244222,    -93222,   30.774,       -   66475.561431, -66475.561431,    0.561431,         '1', '1',                                  '6229-06-28 02:54:28.970117179', '5966-07-09', 'binary', 'original'),
          (4, 1,    -90,   754072151,   3289094, 46114.284799488,  9250340.75,    9250340.75,      9250340.75,        'junkyard', 'junkyard',                    '2002-05-10 05:29:48.990818073', '1815-05-06', 'binary', 'original');

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,b from part_change_various_various_varchar_trunc order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table part_change_various_various_varchar_trunc replace columns (insert_num int, c1 VARCHAR(8), c2 VARCHAR(8), c3 VARCHAR(8), c4 VARCHAR(8), c5 VARCHAR(8), c6 VARCHAR(8), c7 VARCHAR(8), c8 VARCHAR(8), c9 VARCHAR(8), c10 VARCHAR(8), c11 VARCHAR(8), c12 VARCHAR(8), c13 VARCHAR(8), b STRING);

insert into table part_change_various_various_varchar_trunc partition(part=2)
    values (5, 'true', '400',  '44388',       -'100',    '953967041.',       '62.079153',     '718.78',         '1',                'verdict', 'verdict', 'timestamp', 'date', 'binary', 'new');

insert into table part_change_various_various_varchar_trunc partition(part=1)
    values (6,-'false', '-67', '833',          '63993', ' 1255178165.77663', '905070.974', '-4314.7918',        -'1240033819',      'trial',   'trial',  '2016-03-07 03:02:22.0', '2016-03-07', 'binary', 'new');

explain
select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,b from part_change_various_various_varchar_trunc order by insert_num;

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,b from part_change_various_various_varchar_trunc order by insert_num;

drop table part_change_various_various_varchar_trunc;



--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for Various --> Various: (BOOLEAN, TINYINT, SMALLINT, INT, LONG, FLOAT, DOUBLE, DECIMAL(38,18), STRING, CHAR, VARCHAR, DATE) --> TIMESTAMP
--
CREATE TABLE part_change_various_various_timestamp(insert_num int, c1 BOOLEAN, c2 TINYINT, c3 SMALLINT, c4 INT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 DECIMAL(38,18), c9 STRING, c10 CHAR(25), c11 VARCHAR(25), c12 DATE, b STRING) PARTITIONED BY(part INT);

insert into table part_change_various_various_timestamp partition(part=1)
    values(1, true,  200,  72909,      3244222, -99999999999,     -29.0764,      470614135,        470614135,         '0004-09-22 18:26:29.519542222', '0004-09-22 18:26:29.519542222',  '0004-09-22 18:26:29.519542222', '2007-02-09', 'original'),
          (2, 0, 100, 32767, -23372036854775, -9223372036854775808.0, -9223372036854775808.0, 9223372036854775807.0, '', '', '', '2007-02-09 05:17:29.368756876', 'original'),
          (3, 0, 100, -32768, 23372036854775, -9223372036854775809.0, -9223372036854775809.0, 9223372036854775808.0, '', '', '', '2007-02-09 05:17:29.368756876', 'original'),
          (4, false, 72,   3244222,    -93222,   30.774,       -   66475.561431, -66475.561431,    0.561431,          '6229-06-28 02:54:28.970117179', '6229-06-28 02:54:28.970117179',  '6229-06-28 02:54:28.970117179', '5966-07-09', 'original'),
          (5, 1,    -90,   754072151,   3289094, 46114.284799488,  9250340.75,    9250340.75,      9250340.75,        '2002-05-10 05:29:48.990818073', '2002-05-10 05:29:48.990818073',  '2002-05-10 05:29:48.990818073', '1815-05-06', 'original');

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,b from part_change_various_various_timestamp order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table part_change_various_various_timestamp replace columns (insert_num int, c1 TIMESTAMP, c2 TIMESTAMP, c3 TIMESTAMP, c4 TIMESTAMP, c5 TIMESTAMP, c6 TIMESTAMP, c7 TIMESTAMP, c8 TIMESTAMP, c9 TIMESTAMP, c10 TIMESTAMP, c11 TIMESTAMP, c12 TIMESTAMP, b STRING);

insert into table part_change_various_various_timestamp partition(part=2)
    values (6, 'true', '400',  '44388',       -'100',    '953967041.',       '62.079153',     '718.78',         '1',                'timestamp', 'timestamp', 'timestamp', 'date', 'new');

insert into table part_change_various_various_timestamp partition(part=1)
    values (7,-'false', '-67', '833',          '63993', ' 1255178165.77663', '905070.974', '-4314.7918',        -'1240033819',      '2016-03-07 03:02:22.0',   '2016-03-07 03:02:22.0',  '2016-03-07 03:02:22.0', '2016-03-07', 'new');

explain
select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,b from part_change_various_various_timestamp order by insert_num;

select insert_num,part,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,b from part_change_various_various_timestamp order by insert_num;

drop table part_change_various_various_timestamp;


--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for Various --> Various: (STRING, CHAR, VARCHAR, TIMESTAMP --> DATE
--
CREATE TABLE part_change_various_various_date(insert_num int, c1 STRING, c2 CHAR(25), c3 VARCHAR(25), c4 TIMESTAMP, b STRING) PARTITIONED BY(part INT);

insert into table part_change_various_various_date partition(part=1)
    values(1, '0004-09-22 18:26:29.519542222', '0004-09-22 18:26:29.519542222',  '0004-09-22 18:26:29.519542222', '0004-09-22 18:26:29.519542222', 'original'),
          (2, '2007-02-09 05:17:29.368756876', '2007-02-09 05:17:29.368756876',  '2007-02-09 05:17:29.368756876', '2007-02-09 05:17:29.368756876', 'original'),
          (3, '6229-06-28 02:54:28.970117179', '6229-06-28 02:54:28.970117179',  '6229-06-28 02:54:28.970117179', '6229-06-28 02:54:28.970117179', 'original'),
          (4, '2002-05-10 05:29:48.990818073', '2002-05-10 05:29:48.990818073',  '2002-05-10 05:29:48.990818073', '2002-05-10 05:29:48.990818073', 'original');

select insert_num,part,c1,c2,c3,c4,b from part_change_various_various_date order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table part_change_various_various_date replace columns (insert_num int, c1 DATE, c2 DATE, c3 DATE, c4 DATE, b STRING);

insert into table part_change_various_various_date partition(part=2)
    values (5, '2016-03-07', '2016-03-07', '2016-03-07', '2016-03-07', 'new');

insert into table part_change_various_various_date partition(part=1)
    values (6,-'2002-05-10', '2002-05-10', '2002-05-10', '2002-05-10','new');

explain
select insert_num,part,c1,c2,c3,c4,b from part_change_various_various_date order by insert_num;

select insert_num,part,c1,c2,c3,c4,b from part_change_various_various_date order by insert_num;

drop table part_change_various_various_date;


--
-- SUBSECTION: ALTER TABLE CHANGE COLUMNS for Same Type (CHAR, VARCHAR, DECIMAL) --> Different maxLength or precision/scale
--
CREATE TABLE part_change_same_type_different_params(insert_num int, c1 CHAR(12), c2 CHAR(25), c3 VARCHAR(25), c4 VARCHAR(10), c5 DECIMAL(12,4), c6 DECIMAL(20,10), b STRING) PARTITIONED BY(part INT);

CREATE TABLE same_type1_a_txt(insert_num int, c1 CHAR(12), c2 CHAR(25), c3 VARCHAR(25), c4 VARCHAR(10), c5 DECIMAL(12,4), c6 DECIMAL(20,10), b STRING)
row format delimited fields terminated by '|'
stored as textfile;
load data local inpath '../../data/files/same_type1_a.txt' overwrite into table same_type1_a_txt;

select * from same_type1_a_txt;

insert into table part_change_same_type_different_params partition(part=1) select * from same_type1_a_txt;

select insert_num,part,c1,c2,c3,c4,c5,c6,b from part_change_same_type_different_params order by insert_num;

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table part_change_same_type_different_params replace columns (insert_num int, c1 CHAR(8), c2 CHAR(32), c3 VARCHAR(15), c4 VARCHAR(18), c5 DECIMAL(10,2), c6 DECIMAL(25,15), b STRING);

CREATE TABLE same_type1_b_txt(insert_num int, c1 CHAR(8), c2 CHAR(32), c3 VARCHAR(15), c4 VARCHAR(18), c5 DECIMAL(10,2), c6 DECIMAL(25,15), b STRING)
row format delimited fields terminated by '|'
stored as textfile;
load data local inpath '../../data/files/same_type1_b.txt' overwrite into table same_type1_b_txt;

select * from same_type1_b_txt;

insert into table part_change_same_type_different_params partition(part=1) select * from same_type1_b_txt;

CREATE TABLE same_type1_c_txt(insert_num int, c1 CHAR(8), c2 CHAR(32), c3 VARCHAR(15), c4 VARCHAR(18), c5 DECIMAL(10,2), c6 DECIMAL(25,15), b STRING)
row format delimited fields terminated by '|'
stored as textfile;
load data local inpath '../../data/files/same_type1_c.txt' overwrite into table same_type1_c_txt;

select * from same_type1_c_txt;

insert into table part_change_same_type_different_params partition(part=2) select * from same_type1_c_txt;

explain
select insert_num,part,c1,c2,c3,c4,c5,c6,b from part_change_same_type_different_params order by insert_num;

select insert_num,part,c1,c2,c3,c4,c5,c6,b from part_change_same_type_different_params order by insert_num;

drop table part_change_same_type_different_params;
