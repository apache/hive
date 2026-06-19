set hive.stats.column.autogather=false;
set hive.stats.fetch.column.stats=true; 
set hive.compute.query.using.stats=true; 
set hive.mapred.mode=nonstrict;

drop table calendar_n0;

CREATE TABLE calendar_n0 (year int, month int);

insert into calendar_n0 values (2010, 10), (2011, 11), (2012, 12); 

desc formatted calendar_n0;

analyze table calendar_n0 compute statistics;

desc formatted calendar_n0;

explain select count(1) from calendar_n0; 

explain select max(year) from calendar_n0; 

select max(year) from calendar_n0; 

select max(month) from calendar_n0;

analyze table calendar_n0 compute statistics for columns;

desc formatted calendar_n0;

explain select max(year) from calendar_n0; 

select max(year) from calendar_n0;

insert into calendar_n0 values (2015, 15);

desc formatted calendar_n0;

explain select max(year) from calendar_n0; 

select max(year) from calendar_n0;

explain select max(month) from calendar_n0; 

select max(month) from calendar_n0;

analyze table calendar_n0 compute statistics for columns year;

desc formatted calendar_n0;

explain select max(year) from calendar_n0; 

select max(year) from calendar_n0;

explain select max(month) from calendar_n0; 

select max(month) from calendar_n0;

analyze table calendar_n0 compute statistics for columns month;

desc formatted calendar_n0;

explain select max(month) from calendar_n0; 

select max(month) from calendar_n0;

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

create table t_n31 (key string, value string);

load data local inpath '../../data/files/kv1.txt' into table t_n31;

desc formatted t_n31;

analyze table t_n31 compute statistics;

desc formatted t_n31;




