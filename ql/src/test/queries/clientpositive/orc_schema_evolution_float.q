set hive.optimize.index.filter=false;
set hive.metastore.disallow.incompatible.col.type.changes=false;

drop table float_text;
create table float_text(f float);
insert into float_text values(74.72);
insert into float_text values(0.22);
select f from float_text;
alter table float_text change column f f double;
select f from float_text;
select f from float_text where f=74.72;
select f from float_text where f=0.22;
alter table float_text change column f f decimal(14,5);
select f from float_text;
select f from float_text where f=74.72;
select f from float_text where f=0.22;

create table float_orc(f float) stored as orc;
insert overwrite table float_orc select * from float_text;
select f from float_orc;
alter table float_orc change column f f double;
select f from float_orc;
select f from float_orc where f=74.72;
select f from float_orc where f=0.22;
set hive.optimize.index.filter=true;
select f from float_orc where f=74.72;
select f from float_orc where f=0.22;

alter table float_orc change column f f decimal(14,5);
select f from float_orc;
select f from float_orc where f=74.72;
select f from float_orc where f=0.22;
set hive.optimize.index.filter=true;
select f from float_orc where f=74.72;
select f from float_orc where f=0.22;

drop table float_text;
drop table float_orc;
