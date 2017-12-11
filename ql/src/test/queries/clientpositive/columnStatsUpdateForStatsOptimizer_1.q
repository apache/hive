set hive.stats.column.autogather=false;
set hive.stats.fetch.column.stats=true; 
set hive.compute.query.using.stats=true; 
set hive.mapred.mode=nonstrict;

drop table calendar;

CREATE TABLE calendar (year int, month int);

insert into calendar values (2010, 10), (2011, 11), (2012, 12); 

desc formatted calendar;

analyze table calendar compute statistics;

desc formatted calendar;

explain select count(1) from calendar; 

explain select max(year) from calendar; 

select max(year) from calendar; 

select max(month) from calendar;

analyze table calendar compute statistics for columns;

desc formatted calendar;

explain select max(year) from calendar; 

select max(year) from calendar;

insert into calendar values (2015, 15);

desc formatted calendar;

explain select max(year) from calendar; 

select max(year) from calendar;

explain select max(month) from calendar; 

select max(month) from calendar;

analyze table calendar compute statistics for columns year;

desc formatted calendar;

explain select max(year) from calendar; 

select max(year) from calendar;

explain select max(month) from calendar; 

select max(month) from calendar;

analyze table calendar compute statistics for columns month;

desc formatted calendar;

explain select max(month) from calendar; 

select max(month) from calendar;

CREATE TABLE calendarp (`year` int)  partitioned by (p int);

insert into table calendarp partition (p=1) values (2010), (2011), (2012); 

desc formatted calendarp partition (p=1);

explain select max(year) from calendarp where p=1; 

select max(year) from calendarp where p=1; 

analyze table calendarp partition (p=1) compute statistics for columns;

desc formatted calendarp partition (p=1);

explain select max(year) from calendarp where p=1; 

insert into table calendarp partition (p=1) values (2015);

desc formatted calendarp partition (p=1);

explain select max(year) from calendarp where p=1; 

select max(year) from calendarp where p=1;

create table t (key string, value string);

load data local inpath '../../data/files/kv1.txt' into table t;

desc formatted t;

analyze table t compute statistics;

desc formatted t;




