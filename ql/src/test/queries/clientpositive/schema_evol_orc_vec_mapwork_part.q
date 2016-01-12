set hive.cli.print.header=true;
SET hive.exec.schema.evolution=true;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=more;
set hive.exec.dynamic.partition.mode=nonstrict;


-- SORT_QUERY_RESULTS
--
-- FILE VARIATION: ORC, Vectorized, MapWork, Partitioned
--
--
-- SECTION VARIATION: ALTER TABLE ADD COLUMNS ... STATIC INSERT
---
CREATE TABLE partitioned1(a INT, b STRING) PARTITIONED BY(part INT) STORED AS ORC;

insert into table partitioned1 partition(part=1) values(1, 'original'),(2, 'original'), (3, 'original'),(4, 'original');

-- Table-Non-Cascade ADD COLUMNS ...
alter table partitioned1 add columns(c int, d string);

insert into table partitioned1 partition(part=2) values(1, 'new', 10, 'ten'),(2, 'new', 20, 'twenty'), (3, 'new', 30, 'thirty'),(4, 'new', 40, 'forty');

insert into table partitioned1 partition(part=1) values(5, 'new', 100, 'hundred'),(6, 'new', 200, 'two hundred');

-- SELECT permutation columns to make sure NULL defaulting works right
select part,a,b from partitioned1;
select part,a,b,c from partitioned1;
select part,a,b,c,d from partitioned1;
select part,a,c,d from partitioned1;
select part,a,d from partitioned1;
select part,c from partitioned1;
select part,d from partitioned1;

--
-- SECTION VARIATION: ALTER TABLE CHANGE COLUMN ... STATIC INSERT
-- smallint = (2-byte signed integer, from -32,768 to 32,767)
--
CREATE TABLE partitioned2(a smallint, b STRING) PARTITIONED BY(part INT) STORED AS ORC;

insert into table partitioned2 partition(part=1) values(1000, 'original'),(6737, 'original'), ('3', 'original'),('4', 'original');

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table partitioned2 change column a a int;

insert into table partitioned2 partition(part=2) values(72909, 'new'),(200, 'new'), (32768, 'new'),(40000, 'new');

insert into table partitioned2 partition(part=1) values(5000, 'new'),(90000, 'new');

select part,a,b from partitioned2;


--
--
-- SECTION VARIATION: ALTER TABLE ADD COLUMNS ... DYNAMIC INSERT
---
CREATE TABLE partitioned3(a INT, b STRING) PARTITIONED BY(part INT) STORED AS ORC;

insert into table partitioned3 partition(part=1) values(1, 'original'),(2, 'original'), (3, 'original'),(4, 'original');

-- Table-Non-Cascade ADD COLUMNS ...
alter table partitioned3 add columns(c int, d string);

insert into table partitioned3 partition(part) values(1, 'new', 10, 'ten', 2),(2, 'new', 20, 'twenty', 2), (3, 'new', 30, 'thirty', 2),(4, 'new', 40, 'forty', 2),
    (5, 'new', 100, 'hundred', 1),(6, 'new', 200, 'two hundred', 1);

-- SELECT permutation columns to make sure NULL defaulting works right
select part,a,b from partitioned1;
select part,a,b,c from partitioned1;
select part,a,b,c,d from partitioned1;
select part,a,c,d from partitioned1;
select part,a,d from partitioned1;
select part,c from partitioned1;
select part,d from partitioned1;


--
-- SECTION VARIATION: ALTER TABLE CHANGE COLUMN ... DYNAMIC INSERT
-- smallint = (2-byte signed integer, from -32,768 to 32,767)
--
CREATE TABLE partitioned4(a smallint, b STRING) PARTITIONED BY(part INT) STORED AS ORC;

insert into table partitioned4 partition(part=1) values(1000, 'original'),(6737, 'original'), ('3', 'original'),('4', 'original');

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table partitioned4 change column a a int;

insert into table partitioned4 partition(part) values(72909, 'new', 2),(200, 'new', 2), (32768, 'new', 2),(40000, 'new', 2),
    (5000, 'new', 1),(90000, 'new', 1);

select part,a,b from partitioned4;


DROP TABLE partitioned1;
DROP TABLE partitioned2;
DROP TABLE partitioned3;
DROP TABLE partitioned4;