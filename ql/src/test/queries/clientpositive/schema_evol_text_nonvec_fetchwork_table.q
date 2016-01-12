set hive.cli.print.header=true;
SET hive.exec.schema.evolution=true;
SET hive.vectorized.execution.enabled=false;
set hive.fetch.task.conversion=more;

-- SORT_QUERY_RESULTS
--
-- FILE VARIATION: ORC, Non-Vectorized, MapWork, Table
--
--
-- SECTION VARIATION: ALTER TABLE ADD COLUMNS
---
CREATE TABLE table1(a INT, b STRING) STORED AS TEXTFILE;

insert into table table1 values(1, 'original'),(2, 'original'), (3, 'original'),(4, 'original');

select a,b from table1;

-- ADD COLUMNS
alter table table1 add columns(c int, d string);

insert into table table1 values(1, 'new', 10, 'ten'),(2, 'new', 20, 'twenty'), (3, 'new', 30, 'thirty'),(4, 'new', 40, 'forty');

select a,b,c,d from table1;

-- ADD COLUMNS
alter table table1 add columns(e string);

insert into table table1 values(5, 'new', 100, 'hundred', 'another1'),(6, 'new', 200, 'two hundred', 'another2');

select a,b,c,d,e from table1;


--
-- SECTION VARIATION: ALTER TABLE CHANGE COLUMN
-- smallint = (2-byte signed integer, from -32,768 to 32,767)
--
CREATE TABLE table3(a smallint, b STRING) STORED AS TEXTFILE;

insert into table table3 values(1000, 'original'),(6737, 'original'), ('3', 'original'),('4', 'original');

select a,b from table3;

-- ADD COLUMNS ... RESTRICT
alter table table3 change column a a int;

insert into table table3 values(72909, 'new'),(200, 'new'), (32768, 'new'),(40000, 'new');

select a,b from table3;

-- ADD COLUMNS ... RESTRICT
alter table table3 add columns(e string);

insert into table table3 values(5000, 'new', 'another5'),(90000, 'new', 'another6');

select a,b from table3;


-- ADD COLUMNS ... RESTRICT
alter table table3 change column a a int;

select a,b from table3;


DROP TABLE table1;
DROP TABLE table2;
DROP TABLE table3;