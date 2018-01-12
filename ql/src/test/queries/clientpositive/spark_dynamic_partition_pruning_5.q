set hive.spark.dynamic.partition.pruning=true;

-- This qfile tests whether we can handle nested DPP sinks

create table part1(key string, value string) partitioned by (p string);
insert into table part1 partition (p='1') select * from src;

create table part2(key string, value string) partitioned by (p string);
insert into table part2 partition (p='1') select * from src;

create table regular1 as select * from src limit 2;

-- nested DPP is removed, upper most DPP is w/ common join
explain select * from src join part1 on src.key=part1.p join part2 on src.value=part2.p;

-- nested DPP is removed, upper most DPP is w/ map join
set hive.auto.convert.join=true;
-- ensure regular1 is treated as small table, and partitioned tables are not
set hive.auto.convert.join.noconditionaltask.size=20;
explain select * from regular1 join part1 on regular1.key=part1.p join part2 on regular1.value=part2.p;

drop table part1;
drop table part2;
drop table regular1;