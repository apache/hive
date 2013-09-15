drop table alter_varchar_1;

create table alter_varchar_1 (key string, value string);
insert overwrite table alter_varchar_1
  select key, value from src limit 5;

select * from alter_varchar_1;

-- change column to varchar
alter table alter_varchar_1 change column value value varchar(20);
-- contents should still look the same
select * from alter_varchar_1;

-- change column to smaller varchar
alter table alter_varchar_1 change column value value varchar(3);
-- value column should be truncated now
select * from alter_varchar_1;

-- change back to bigger varchar
alter table alter_varchar_1 change column value value varchar(20);
-- column values should be full size again
select * from alter_varchar_1;

-- add varchar column
alter table alter_varchar_1 add columns (key2 int, value2 varchar(10));
select * from alter_varchar_1;

insert overwrite table alter_varchar_1
  select key, value, key, value from src limit 5;
select * from alter_varchar_1;

drop table alter_varchar_1;
