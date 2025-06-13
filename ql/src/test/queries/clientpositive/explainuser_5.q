set hive.explain.user=true;
set hive.auto.convert.join=true;
set hive.optimize.dynamic.partition.hashjoin=true;
set hive.auto.convert.join.noconditionaltask.size=3000;

create table small (key string, value string);
create table middle (key string, value string);
create table big (key string, value string);

alter table small update statistics set('numRows'='8', 'rawDataSize'='100');
alter table middle update statistics set('numRows'='28', 'rawDataSize'='2000');
alter table big update statistics set('numRows'='1234560', 'rawDataSize'='12345670');

explain
with first as (
  select big.key + middle.key as a, big.value + middle.value as b from big, middle where big.key = middle.key
)
select first.a, small.value from first, small where first.b = small.key;
