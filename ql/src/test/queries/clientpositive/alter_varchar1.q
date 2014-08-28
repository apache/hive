-- SORT_QUERY_RESULTS

create database avc;

create table avc.alter_varchar_1 (key string, value string);
insert overwrite table avc.alter_varchar_1
  select key, value from src order by key limit 5;

select * from avc.alter_varchar_1;

-- change column to varchar
alter table avc.alter_varchar_1 change column value value varchar(20);
-- contents should still look the same
select * from avc.alter_varchar_1;

-- change column to smaller varchar
alter table avc.alter_varchar_1 change column value value varchar(3);
-- value column should be truncated now
select * from avc.alter_varchar_1;

-- change back to bigger varchar
alter table avc.alter_varchar_1 change column value value varchar(20);
-- column values should be full size again
select * from avc.alter_varchar_1;

-- add varchar column
alter table avc.alter_varchar_1 add columns (key2 int, value2 varchar(10));
select * from avc.alter_varchar_1;

insert overwrite table avc.alter_varchar_1
  select key, value, key, value from src order by key limit 5;
select * from avc.alter_varchar_1;

drop table avc.alter_varchar_1;
drop database avc;
