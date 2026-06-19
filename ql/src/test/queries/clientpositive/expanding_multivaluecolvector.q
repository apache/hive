SET hive.vectorized.adaptor.usage.mode=all;
SET hive.stats.autogather=false;
SET hive.fetch.task.conversion=none;
SET hive.create.as.acid=true;
SET hive.explain.user=false;
SET hive.vectorized.use.vectorized.input.format=false;
SET hive.vectorized.use.vector.serde.deserialize=true;
SET hive.vectorized.use.row.serde.deserialize=true;

create external table source (id int, str string) row format delimited fields terminated by ',' stored as textfile location '../../data/files/splittablecolumn/';

create table table_with_list (id int, val array<string>) stored as orc;

-- split() causes vectorized row batch to expand larger than default 1024
explain vectorization detail
insert overwrite table table_with_list select id, split(str, '#') from source;

insert overwrite table table_with_list select id, split(str, '#') from source;

select * from table_with_list order by id desc limit 10;

-- make sure that partition columns work fine when vectorized row batch expands
create table source_partitioned (id int, str string) partitioned by (part string)
row format delimited fields terminated by ',' stored as textfile;

load data local inpath '../../data/files/splittablecolumn/lines1to5000.txt'
overwrite into table source_partitioned partition (part='a');

load data local inpath '../../data/files/splittablecolumn/lines5001to9000.txt'
overwrite into table source_partitioned partition (part='b');

create table table_partitioned (id int, val int) partitioned by (part string) stored as orc;

explain vectorization detail
insert overwrite table table_partitioned select id, split(str, '#')[2], part from source_partitioned;

insert overwrite table table_partitioned select id, split(str, '#')[2], part from source_partitioned;

select * from table_partitioned where part='a' order by id desc limit 10;
select * from table_partitioned where part='b' order by id desc limit 10;
select count(*) from table_partitioned where part='a';
select count(*) from table_partitioned where part='b';

