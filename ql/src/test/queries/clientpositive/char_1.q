set hive.strict.checks.bucketing=false;

-- SORT_QUERY_RESULTS

drop table char1;
drop table char1_1;

create table char1 (key char(10), value char(20));
create table char1_1 (key string, value string);

-- load from file
load data local inpath '../../data/files/srcbucket0.txt' overwrite into table char1;
select * from char1 order by key, value limit 2;

-- insert overwrite, from same/different length char
insert overwrite table char1
  select cast(key as char(10)), cast(value as char(15)) from src order by key, value limit 2;
select key, value from char1;

-- insert overwrite, from string
insert overwrite table char1
  select key, value from src order by key, value limit 2;
select key, value from char1;

-- insert string from char
insert overwrite table char1_1
  select key, value from char1 order by key, value limit 2;
select key, value from char1_1;

-- respect string length
insert overwrite table char1 
  select key, cast(value as char(3)) from src order by key, value limit 2;
select key, value from char1;

drop table char1;
drop table char1_1;
