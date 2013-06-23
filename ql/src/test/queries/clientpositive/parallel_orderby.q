create table src5 (key string, value string);
load data local inpath '../data/files/kv5.txt' into table src5;
load data local inpath '../data/files/kv5.txt' into table src5;

set mapred.reduce.tasks = 4;
set hive.optimize.sampling.orderby=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

explain
create table total_ordered as select * from src5 order by key, value;
create table total_ordered as select * from src5 order by key, value;

desc formatted total_ordered;
select * from total_ordered;
