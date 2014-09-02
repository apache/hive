-- SORT_QUERY_RESULTS

create database ac;

create table ac.alter_char_1 (key string, value string);
insert overwrite table ac.alter_char_1
  select key, value from src order by key limit 5;

select * from ac.alter_char_1;

-- change column to char
alter table ac.alter_char_1 change column value value char(20);
-- contents should still look the same
select * from ac.alter_char_1;

-- change column to smaller char
alter table ac.alter_char_1 change column value value char(3);
-- value column should be truncated now
select * from ac.alter_char_1;

-- change back to bigger char
alter table ac.alter_char_1 change column value value char(20);
-- column values should be full size again
select * from ac.alter_char_1;

-- add char column
alter table ac.alter_char_1 add columns (key2 int, value2 char(10));
select * from ac.alter_char_1;

insert overwrite table ac.alter_char_1
  select key, value, key, value from src order by key limit 5;
select * from ac.alter_char_1;

drop table ac.alter_char_1;
drop database ac;
