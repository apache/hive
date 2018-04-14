--! qt:dataset:part
set hive.stats.column.autogather=true;
set hive.mapred.mode=nonstrict;
set hive.cli.print.header=true;
SET hive.exec.schema.evolution=true;
SET hive.vectorized.execution.enabled=false;
set hive.fetch.task.conversion=none;
set hive.exec.dynamic.partition.mode=nonstrict;

-- SORT_QUERY_RESULTS
--
-- FILE VARIATION: TEXT, Non-Vectorized, MapWork, Partitioned
--
--
-- SECTION VARIATION: ALTER TABLE ADD COLUMNS ... STATIC INSERT
---
CREATE TABLE partitioned1_n1(a INT, b STRING) PARTITIONED BY(part INT) STORED AS TEXTFILE;

explain insert into table partitioned1_n1 partition(part=1) values(1, 'original'),(2, 'original'), (3, 'original'),(4, 'original');

insert into table partitioned1_n1 partition(part=1) values(1, 'original'),(2, 'original'), (3, 'original'),(4, 'original');

desc formatted partitioned1_n1 partition(part=1);

desc formatted partitioned1_n1 partition(part=1) a;

-- Table-Non-Cascade ADD COLUMNS ...
alter table partitioned1_n1 add columns(c int, d string);

desc formatted partitioned1_n1 partition(part=1);

explain insert into table partitioned1_n1 partition(part=2) values(1, 'new', 10, 'ten'),(2, 'new', 20, 'twenty'), (3, 'new', 30, 'thirty'),(4, 'new', 40, 'forty');

insert into table partitioned1_n1 partition(part=2) values(1, 'new', 10, 'ten'),(2, 'new', 20, 'twenty'), (3, 'new', 30, 'thirty'),(4, 'new', 40, 'forty');

desc formatted partitioned1_n1 partition(part=2);

desc formatted partitioned1_n1 partition(part=2) c;

explain insert into table partitioned1_n1 partition(part=1) values(5, 'new', 100, 'hundred'),(6, 'new', 200, 'two hundred');

insert into table partitioned1_n1 partition(part=1) values(5, 'new', 100, 'hundred'),(6, 'new', 200, 'two hundred');

desc formatted partitioned1_n1 partition(part=1);

desc formatted partitioned1_n1 partition(part=1) a;

desc formatted partitioned1_n1 partition(part=1) c;
