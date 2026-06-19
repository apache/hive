--! qt:dataset:src
set hive.fetch.task.conversion=more;

drop table if exists decimal_1_n0;

create table decimal_1_n0 (t decimal(4,2), u decimal(5), v decimal);
alter table decimal_1_n0 set serde 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe';

desc decimal_1_n0;

insert overwrite table decimal_1_n0
  select cast('17.29' as decimal(4,2)), 3.1415926BD, 3115926.54321BD from src tablesample (1 rows);
select cast(t as boolean) from decimal_1_n0;
select cast(t as tinyint) from decimal_1_n0;
select cast(t as smallint) from decimal_1_n0;
select cast(t as int) from decimal_1_n0;
select cast(t as bigint) from decimal_1_n0;
select cast(t as float) from decimal_1_n0;
select cast(t as double) from decimal_1_n0;
select cast(t as string) from decimal_1_n0;
select cast(t as timestamp) from decimal_1_n0;

drop table decimal_1_n0;
