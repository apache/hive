set hive.vectorized.execution.enabled=false;
set hive.fetch.task.conversion=none;

drop table orc_decimal;
drop table staging;
create table orc_decimal (id decimal(18,0)) stored as orc;

create table staging (id decimal(18,0));

insert into staging values (34324.0), (100000000.0), (200000000.0), (300000000.0);

insert overwrite table orc_decimal select id from staging;

set hive.vectorized.execution.enabled=true;

explain vectorization expression
select * from orc_decimal where id in ('100000000', '200000000');
select * from orc_decimal where id in ('100000000', '200000000');

drop table orc_decimal;
drop table staging;
