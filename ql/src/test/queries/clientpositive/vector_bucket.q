set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.support.concurrency=true;
set hive.fetch.task.conversion=none;


CREATE TABLE non_orc_table(a INT, b STRING) CLUSTERED BY(a) INTO 2 BUCKETS STORED AS sequencefile; 


explain vectorization expression
insert into table non_orc_table values(1, 'one'),(1, 'one'), (2, 'two'),(3, 'three'); select a, b from non_orc_table order by a;

insert into table non_orc_table values(1, 'one'),(1, 'one'), (2, 'two'),(3, 'three'); select a, b from non_orc_table order by a;
