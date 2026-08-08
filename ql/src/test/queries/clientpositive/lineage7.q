set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.LineageLogger;

create table table_1 (id1 int, id2 int, key string);

create table table_2 as select
sum(id1) over(partition by key ) sum1,
sum(id2) over(partition by key ) sum2
from table_1