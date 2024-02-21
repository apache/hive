set hive.cbo.fallback.strategy=NEVER;
-- setting up a table with multiple rows
drop table if exists HIVE_20262;
create table HIVE_20262 (a array<int>);
insert into HIVE_20262 select array(1);
insert into HIVE_20262 select array(2);


set hive.stats.udtf.factor=5;

-- Test when input has a single row
explain select explode(array(1,2,3,4,5)) as col;

-- Test when input has multiple rows
explain select explode(a) from HIVE_20262;

-- the output data size should increase
explain select 1, r from HIVE_20262
      lateral view explode(a) t as r ;

-- Default behaviour tests:

-- 1 is the default value
set hive.stats.udtf.factor=1;

-- Test when input has a single row
explain select explode(array(1,2,3,4,5)) as col;

-- Test when input has multiple rows
explain select explode(a) from HIVE_20262;


