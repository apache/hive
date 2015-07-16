set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=minimal;

drop table decimal_2;

create table decimal_2 (t decimal(18,9)) stored as orc;

insert overwrite table decimal_2
  select cast('17.29' as decimal(4,2)) from src tablesample (1 rows);

explain
select cast(t as boolean) from decimal_2 order by t;

select cast(t as boolean) from decimal_2 order by t;

explain
select cast(t as tinyint) from decimal_2 order by t;

select cast(t as tinyint) from decimal_2 order by t;

explain
select cast(t as smallint) from decimal_2 order by t;

select cast(t as smallint) from decimal_2 order by t;

explain
select cast(t as int) from decimal_2 order by t;

select cast(t as int) from decimal_2 order by t;

explain
select cast(t as bigint) from decimal_2 order by t;

select cast(t as bigint) from decimal_2 order by t;

explain
select cast(t as float) from decimal_2 order by t;

select cast(t as float) from decimal_2 order by t;

explain
select cast(t as double) from decimal_2 order by t;

select cast(t as double) from decimal_2 order by t;

explain
select cast(t as string) from decimal_2 order by t;

select cast(t as string) from decimal_2 order by t;

insert overwrite table decimal_2
  select cast('3404045.5044003' as decimal(18,9)) from src tablesample (1 rows);

explain
select cast(t as boolean) from decimal_2 order by t;

select cast(t as boolean) from decimal_2 order by t;

explain
select cast(t as tinyint) from decimal_2 order by t;

select cast(t as tinyint) from decimal_2 order by t;

explain
select cast(t as smallint) from decimal_2 order by t;

select cast(t as smallint) from decimal_2 order by t;

explain
select cast(t as int) from decimal_2 order by t;

select cast(t as int) from decimal_2 order by t;

explain
select cast(t as bigint) from decimal_2 order by t;

select cast(t as bigint) from decimal_2 order by t;

explain
select cast(t as float) from decimal_2 order by t;

select cast(t as float) from decimal_2 order by t;

explain
select cast(t as double) from decimal_2 order by t;

select cast(t as double) from decimal_2 order by t;

explain
select cast(t as string) from decimal_2 order by t;

select cast(t as string) from decimal_2 order by t;

explain
select cast(3.14 as decimal(4,2)) as c from decimal_2 order by c;

select cast(3.14 as decimal(4,2)) as c from decimal_2 order by c;

explain
select cast(cast(3.14 as float) as decimal(4,2)) as c from decimal_2 order by c;

select cast(cast(3.14 as float) as decimal(4,2)) as c from decimal_2 order by c;

explain
select cast(cast('2012-12-19 11:12:19.1234567' as timestamp) as decimal(30,8)) as c from decimal_2 order by c;

select cast(cast('2012-12-19 11:12:19.1234567' as timestamp) as decimal(30,8)) as c from decimal_2 order by c;

explain
select cast(true as decimal) as c from decimal_2 order by c;

explain
select cast(true as decimal) as c from decimal_2 order by c;

select cast(true as decimal) as c from decimal_2 order by c;

explain
select cast(3Y as decimal) as c from decimal_2 order by c;

select cast(3Y as decimal) as c from decimal_2 order by c;

explain
select cast(3S as decimal) as c from decimal_2 order by c;

select cast(3S as decimal) as c from decimal_2 order by c;

explain
select cast(cast(3 as int) as decimal) as c from decimal_2 order by c;

select cast(cast(3 as int) as decimal) as c from decimal_2 order by c;

explain
select cast(3L as decimal) as c from decimal_2 order by c;

select cast(3L as decimal) as c from decimal_2 order by c;

explain
select cast(0.99999999999999999999 as decimal(20,19)) as c from decimal_2 order by c;

select cast(0.99999999999999999999 as decimal(20,19)) as c from decimal_2 order by c;

explain
select cast('0.99999999999999999999' as decimal(20,20)) as c from decimal_2 order by c;

select cast('0.99999999999999999999' as decimal(20,20)) as c from decimal_2 order by c;
drop table decimal_2;
