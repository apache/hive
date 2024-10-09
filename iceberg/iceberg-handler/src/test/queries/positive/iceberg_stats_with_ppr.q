set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.analyze.stmt.collect.partlevel.stats=false;

create external table ice01 (`i` int, `t` timestamp) 
    partitioned by (year int, month int, day int) 
stored by iceberg tblproperties ('format-version'='2', 'write.summary.partition-limit'='10');

insert into ice01 (i, year, month, day) values
(1, 2023, 10, 3),
(2, 2023, 10, 3),
(2, 2023, 10, 3),
(3, 2023, 10, 4),
(4, 2023, 10, 4);

explain
select i from ice01 where year=2023 and month = 10 and day = 3;