--! qt:dataset:src
SET hive.vectorized.execution.enabled=false;
set hive.spark.dynamic.partition.pruning=true;

-- This qfile tests whether we can handle nested DPP sinks

create table part1_n0(key string, value string) partitioned by (p string);
insert into table part1_n0 partition (p='1') select * from src;

create table part2_n3(key string, value string) partitioned by (p string);
insert into table part2_n3 partition (p='1') select * from src;

create table regular1 as select * from src limit 2;

-- nested DPP is removed, upper most DPP is w/ common join
explain select * from src join part1_n0 on src.key=part1_n0.p join part2_n3 on src.value=part2_n3.p;

-- nested DPP is removed, upper most DPP is w/ map join
set hive.auto.convert.join=true;
-- ensure regular1 is treated as small table, and partitioned tables are not
set hive.auto.convert.join.noconditionaltask.size=20;
explain select * from regular1 join part1_n0 on regular1.key=part1_n0.p join part2_n3 on regular1.value=part2_n3.p;

drop table part1_n0;
drop table part2_n3;
drop table regular1;