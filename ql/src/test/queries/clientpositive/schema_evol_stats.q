set hive.mapred.mode=nonstrict;
SET hive.exec.schema.evolution=true;
set hive.llap.io.enabled=false;

CREATE TABLE partitioned1(a INT, b STRING) PARTITIONED BY(part INT) STORED AS TEXTFILE;

insert into table partitioned1 partition(part=1) values(1, 'original'),(2, 'original'), (3, 'original'),(4, 'original');

-- Table-Non-Cascade ADD COLUMNS ...
alter table partitioned1 add columns(c int, d string);

insert into table partitioned1 partition(part=2) values(1, 'new', 10, 'ten'),(2, 'new', NULL, 'twenty'), (3, 'new', 30, 'thirty'),(4, 'new', 40, 'forty');

analyze table partitioned1 compute statistics for columns;

desc formatted partitioned1;

desc formatted partitioned1 PARTITION(part=1);

desc formatted partitioned1 PARTITION(part=2);

set hive.compute.query.using.stats=true;

explain select count(c) from partitioned1;

select count(c) from partitioned1;

drop table partitioned1;

CREATE TABLE partitioned1(a INT, b STRING) PARTITIONED BY(part INT) STORED AS ORC;

insert into table partitioned1 partition(part=1) values(1, 'original'),(2, 'original'), (3, 'original'),(4, 'original');

-- Table-Non-Cascade ADD COLUMNS ...
alter table partitioned1 add columns(c int, d string);

insert into table partitioned1 partition(part=2) values(1, 'new', 10, 'ten'),(2, 'new', NULL, 'twenty'), (3, 'new', 30, 'thirty'),(4, 'new', 40, 'forty');

analyze table partitioned1 compute statistics for columns;

desc formatted partitioned1;

desc formatted partitioned1 PARTITION(part=1);

desc formatted partitioned1 PARTITION(part=2);

set hive.compute.query.using.stats=true;

explain select count(c) from partitioned1;

select count(c) from partitioned1;
