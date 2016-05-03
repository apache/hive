set hive.fetch.task.conversion=none;
SET hive.exec.schema.evolution=true;

create table src_orc (key smallint, val string) stored as orc;
create table src_orc2 (key smallint, val string) stored as orc;

-- integer type widening
insert overwrite table src_orc select * from src;
select sum(hash(*)) from src_orc;

alter table src_orc change key key smallint;
select sum(hash(*)) from src_orc;

alter table src_orc change key key int;
select sum(hash(*)) from src_orc;

alter table src_orc change key key bigint;
select sum(hash(*)) from src_orc;

-- replace columns for adding columns and type widening
insert overwrite table src_orc2 select * from src;
select sum(hash(*)) from src_orc2;

alter table src_orc2 replace columns (k smallint, v string);
select sum(hash(*)) from src_orc2;

alter table src_orc2 replace columns (k int, v string);
select sum(hash(*)) from src_orc2;

alter table src_orc2 replace columns (k bigint, v string);
select sum(hash(*)) from src_orc2;

alter table src_orc2 replace columns (k bigint, v string, z int);
select sum(hash(*)) from src_orc2;

alter table src_orc2 replace columns (k bigint, v string, z bigint);
select sum(hash(*)) from src_orc2;

alter table src_orc2 replace columns (k bigint, v string, z bigint, y float);
select sum(hash(*)) from src_orc2;

