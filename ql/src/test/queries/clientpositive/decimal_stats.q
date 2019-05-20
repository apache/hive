--! qt:dataset:src
set hive.stats.fetch.column.stats=true;
drop table if exists decimal_1_n1;

create table decimal_1_n1 (t decimal(4,2), u decimal(5), v decimal);

desc decimal_1_n1;

insert overwrite table decimal_1_n1
  select cast('17.29' as decimal(4,2)), 3.1415926BD, null from src;

analyze table decimal_1_n1 compute statistics for columns;

desc formatted decimal_1_n1 v;

explain select * from decimal_1_n1 order by t limit 100;
drop table decimal_1_n1;
