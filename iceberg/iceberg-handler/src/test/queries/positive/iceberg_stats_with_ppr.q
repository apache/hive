--! qt:replace:/(\s+Statistics\: Num rows\: \d+ Data size\:\s+)\S+(\s+Basic stats\: \S+ Column stats\: \S+)/$1#Masked#$2/

set hive.explain.user=false;
set hive.compute.query.using.stats=true;
set hive.fetch.task.conversion=none;

create external table ice01 (`i` int, `t` timestamp) 
    partitioned by (year int, month int, day int) 
stored by iceberg tblproperties ('format-version'='2');

insert into ice01 (i, year, month, day) values
(1, 2023, 10, 3),
(2, 2023, 10, 3),
(2, 2023, 10, 3),
(3, 2023, 10, 4),
(4, 2023, 10, 4);

explain
select i from ice01 
  where year=2023 and month = 10 and day = 3;

explain 
select count(*) from ice01 
  where year=2023 and month = 10 and day = 3;

select count(1) from ice01 
  where year=2023 and month = 10 and day = 3;

drop table ice01;