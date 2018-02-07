set hive.query.results.cache.enabled=true;

create table rct (key string, value string);
load data local inpath '../../data/files/kv1.txt' overwrite into table rct;

create table rct_part (key string, value string) partitioned by (ds string);
load data local inpath '../../data/files/kv1.txt' overwrite into table rct_part partition (ds="2008-04-08");
load data local inpath '../../data/files/kv1.txt' overwrite into table rct_part partition (ds="2008-04-09");

create temporary table tmptab as select * from src;

select count(*) from tmptab where key = 0;
set test.comment="Query involving temp tables should not be added to the cache";
set test.comment;
explain
select count(*) from tmptab where key = 0;

-- A cached query should not be used if one of the tables used now resolves to a temp table.
select count(*) from rct where key = 0;
set test.comment="Query should use the cache";
set test.comment;
explain
select count(*) from rct where key = 0;
-- Create temp table with same name, which takes precedence over the non-temp table.
create temporary table rct as select * from tmptab;
set test.comment="Cached query does not get used now that it resolves to a temp table";
set test.comment;
explain
select count(*) from rct where key = 0;

-- Try with partitioned table
select count(*) from rct_part where ds="2008-04-08" and key = 0;
set test.comment="Query should use the cache";
set test.comment;
explain
select count(*) from rct_part where ds="2008-04-08" and key = 0;
-- Create temp table with same name, which takes precedence over the non-temp table.
create temporary table rct_part as select key, value, "2008-04-08" as ds from tmptab;
set test.comment="Cached query does not get used now that it resolves to a temp table";
set test.comment;
explain
select count(*) from rct_part where ds="2008-04-08" and key = 0;
