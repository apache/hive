set hive.stats.column.autogather=false;
set hive.stats.fetch.column.stats=true;
set hive.compute.query.using.stats=true; 


drop table calendar;

CREATE TABLE calendar (year int, month int) clustered by (month) into 2 buckets stored as orc;

insert into calendar values (2010, 10), (2011, 11), (2012, 12);

desc formatted calendar;

analyze table calendar compute statistics for columns year;

desc formatted calendar;

explain select max(year) from calendar;

select max(year) from calendar;

explain select count(1) from calendar;

select count(1) from calendar;

ALTER TABLE calendar CHANGE year year1 INT;

--after patch, should be old stats rather than -1

desc formatted calendar;

--but basic/column stats can not be used by optimizer

explain select max(month) from calendar;

select max(month) from calendar;

explain select count(1) from calendar;

select count(1) from calendar;

truncate table calendar;

--after patch, should be 0 

desc formatted calendar;

--but column stats can not be used by optimizer

explain select max(month) from calendar;

select max(month) from calendar;

--basic stats can be used by optimizer

explain select count(1) from calendar;

select count(1) from calendar;
