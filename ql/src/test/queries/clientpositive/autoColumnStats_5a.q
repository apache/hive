set hive.stats.column.autogather=true;
set hive.mapred.mode=nonstrict;
set hive.cli.print.header=true;
SET hive.exec.schema.evolution=true;
SET hive.vectorized.execution.enabled=false;
set hive.fetch.task.conversion=none;
set hive.exec.dynamic.partition.mode=nonstrict;

-- SORT_QUERY_RESULTS

CREATE TABLE partitioned1(a INT, b STRING) PARTITIONED BY(part INT) STORED AS TEXTFILE;

explain extended 
insert into table partitioned1 partition(part=1) values(1, 'original');

insert into table partitioned1 partition(part=1) values(1, 'original');

desc formatted partitioned1 partition(part=1);

explain extended 
insert into table partitioned1 partition(part=1) values(2, 'original'), (3, 'original'),(4, 'original');

insert into table partitioned1 partition(part=1) values(2, 'original'), (3, 'original'),(4, 'original');

explain insert into table partitioned1 partition(part=1) values(1, 'original'),(2, 'original'), (3, 'original'),(4, 'original');

desc formatted partitioned1;
desc formatted partitioned1 partition(part=1);
desc formatted partitioned1 partition(part=1) a;

