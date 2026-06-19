--! qt:dataset:src
set hive.exec.max.dynamic.partitions.pernode=200;
set hive.exec.max.dynamic.partitions=200;
set hive.optimize.sort.dynamic.partition.threshold=-1;

drop table src2_n3;
create table src2_n3 (key int) partitioned by (value string);

-- regular insert overwrite + insert into

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecTezSummaryPrinter;
insert overwrite table src2_n3 partition (value) select * from src where key < 100;
insert into table src2_n3 partition (value) select * from src where key < 200;

drop table src2_n3;
create table src2_n3 (key int) partitioned by (value string);

insert overwrite table src2_n3 partition (value) select * from src where key < 200;
insert into table src2_n3 partition (value) select * from src where key < 300;

-- multi insert overwrite + insert into

drop table src2_n3;
drop table src3_n0;
create table src2_n3 (key int) partitioned by (value string);
create table src3_n0 (key int) partitioned by (value string);

from src
insert overwrite table src2_n3 partition (value) select * where key < 100
insert overwrite table src3_n0 partition (value) select * where key >= 100 and key < 200;

from src
insert into table src2_n3 partition (value) select * where key < 100
insert into table src3_n0 partition (value) select * where key >= 100 and key < 300;

-- union all insert overwrite + insert into

drop table src2_n3;
create table src2_n3 (key int) partitioned by (value string);

insert overwrite table src2_n3 partition (value)
select temps.* from (
  select * from src where key < 100
  union all
  select * from src where key >= 100 and key < 200) temps;

insert into table src2_n3 partition (value)
select temps.* from (
  select * from src where key < 100
  union all
  select * from src where key >= 100 and key < 300) temps;
