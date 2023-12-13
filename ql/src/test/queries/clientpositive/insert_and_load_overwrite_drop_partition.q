CREATE EXTERNAL TABLE `table1`(
  `name` string,
  `number` string)
PARTITIONED BY (
  `part_col` string);

CREATE EXTERNAL TABLE `table2`(
  `name` string,
  `number` string)
PARTITIONED BY (
  `part_col` string);

insert into table table1 values ('a', '10', 'part1');
insert into table table1 values ('b', '11', 'part1');
insert into table table1 values ('a2', '2', 'part2');

insert into table table2 values ('x', '100', 'part1');
insert into table table2 values ('y', '101', 'part1');
insert into table table2 values ('z', '102', 'part1');
insert into table table2 values ('x2', '200', 'part2');
insert into table table2 values ('y2', '201', 'part2');
insert into table table2 values ('x3', '300', 'part3');

--non empty input case
alter table table2 drop partition(part_col='part1');

select count(*) from table2 where part_col='part1';

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/table2/part_col=part1;

insert overwrite table table2 partition(part_col='part1') select name, number from table1 where part_col='part1';

select count(*) from table2 where part_col='part1';

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/table2/part_col=part1;

--empty input case
alter table table2 drop partition(part_col='part2');

select count(*) from table2 where part_col='part2';

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/table2/part_col=part2;

insert overwrite table table2 partition(part_col='part2') select name, number from table1 where part_col='dummy_part';

select count(*) from table2 where part_col='part2';

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/table2/part_col=part2;

--load overwrite partition
alter table table2 drop partition(part_col='part3');

select count(*) from table2 where part_col='part3';

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/table2/part_col=part3;

LOAD DATA LOCAL INPATH '../../data/files/kv5.txt' OVERWRITE INTO TABLE table2 PARTITION(part_col='part3');

select count(*) from table2 where part_col='part3';

dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/table2/part_col=part3;