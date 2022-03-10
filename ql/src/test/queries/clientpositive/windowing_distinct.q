drop table windowing_distinct;

create table windowing_distinct(
           index int,
           t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           `dec` decimal,
           bin binary)
       row format delimited
       fields terminated by '|'
       TBLPROPERTIES ("hive.serialization.decode.binary.as.base64"="false");

load data local inpath '../../data/files/windowing_distinct.txt' into table windowing_distinct;

EXPLAIN CBO
SELECT COUNT(DISTINCT t) OVER (PARTITION BY index),
       COUNT(DISTINCT d) OVER (PARTITION BY index),
       COUNT(DISTINCT bo) OVER (PARTITION BY index),
       COUNT(DISTINCT s) OVER (PARTITION BY index),
       COUNT(DISTINCT concat('Mr.', s)) OVER (PARTITION BY index),
       COUNT(DISTINCT ts) OVER (PARTITION BY index),
       COUNT(DISTINCT `dec`) OVER (PARTITION BY index),
       COUNT(DISTINCT bin) OVER (PARTITION BY index)
FROM windowing_distinct;

EXPLAIN VECTORIZATION
SELECT COUNT(DISTINCT t) OVER (PARTITION BY index),
       COUNT(DISTINCT d) OVER (PARTITION BY index),
       COUNT(DISTINCT bo) OVER (PARTITION BY index),
       COUNT(DISTINCT s) OVER (PARTITION BY index),
       COUNT(DISTINCT concat('Mr.', s)) OVER (PARTITION BY index),
       COUNT(DISTINCT ts) OVER (PARTITION BY index),
       COUNT(DISTINCT `dec`) OVER (PARTITION BY index),
       COUNT(DISTINCT bin) OVER (PARTITION BY index)
FROM windowing_distinct;

SELECT COUNT(DISTINCT t) OVER (PARTITION BY index),
       COUNT(DISTINCT d) OVER (PARTITION BY index),
       COUNT(DISTINCT bo) OVER (PARTITION BY index),
       COUNT(DISTINCT s) OVER (PARTITION BY index),
       COUNT(DISTINCT concat('Mr.', s)) OVER (PARTITION BY index),
       COUNT(DISTINCT ts) OVER (PARTITION BY index),
       COUNT(DISTINCT `dec`) OVER (PARTITION BY index),
       COUNT(DISTINCT bin) OVER (PARTITION BY index)
FROM windowing_distinct;


EXPLAIN CBO
SELECT SUM(DISTINCT t) OVER (PARTITION BY index),
       SUM(DISTINCT d) OVER (PARTITION BY index),
       SUM(DISTINCT s) OVER (PARTITION BY index),
       SUM(DISTINCT concat('Mr.', s)) OVER (PARTITION BY index),
       SUM(DISTINCT ts) OVER (PARTITION BY index),
       SUM(DISTINCT `dec`) OVER (PARTITION BY index)
FROM windowing_distinct;

EXPLAIN VECTORIZATION
SELECT SUM(DISTINCT t) OVER (PARTITION BY index),
       SUM(DISTINCT d) OVER (PARTITION BY index),
       SUM(DISTINCT s) OVER (PARTITION BY index),
       SUM(DISTINCT concat('Mr.', s)) OVER (PARTITION BY index),
       SUM(DISTINCT ts) OVER (PARTITION BY index),
       SUM(DISTINCT `dec`) OVER (PARTITION BY index)
FROM windowing_distinct;

SELECT SUM(DISTINCT t) OVER (PARTITION BY index),
       SUM(DISTINCT d) OVER (PARTITION BY index),
       SUM(DISTINCT s) OVER (PARTITION BY index),
       SUM(DISTINCT concat('Mr.', s)) OVER (PARTITION BY index),
       SUM(DISTINCT ts) OVER (PARTITION BY index),
       SUM(DISTINCT `dec`) OVER (PARTITION BY index)
FROM windowing_distinct;

EXPLAIN CBO
SELECT AVG(DISTINCT t) OVER (PARTITION BY index),
       AVG(DISTINCT d) OVER (PARTITION BY index),
       AVG(DISTINCT s) OVER (PARTITION BY index),
       AVG(DISTINCT concat('Mr.', s)) OVER (PARTITION BY index),
       AVG(DISTINCT ts) OVER (PARTITION BY index),
       AVG(DISTINCT `dec`) OVER (PARTITION BY index)
FROM windowing_distinct;

EXPLAIN VECTORIZATION
SELECT AVG(DISTINCT t) OVER (PARTITION BY index),
       AVG(DISTINCT d) OVER (PARTITION BY index),
       AVG(DISTINCT s) OVER (PARTITION BY index),
       AVG(DISTINCT concat('Mr.', s)) OVER (PARTITION BY index),
       AVG(DISTINCT ts) OVER (PARTITION BY index),
       AVG(DISTINCT `dec`) OVER (PARTITION BY index)
FROM windowing_distinct;

SELECT AVG(DISTINCT t) OVER (PARTITION BY index),
       AVG(DISTINCT d) OVER (PARTITION BY index),
       AVG(DISTINCT s) OVER (PARTITION BY index),
       AVG(DISTINCT concat('Mr.', s)) OVER (PARTITION BY index),
       AVG(DISTINCT ts) OVER (PARTITION BY index),
       AVG(DISTINCT `dec`) OVER (PARTITION BY index)
FROM windowing_distinct;

-- count
select index, f, count(distinct f) over (partition by index order by f rows between 2 preceding and 1 preceding),
                 count(distinct f) over (partition by index order by f rows between unbounded preceding and 1 preceding),
                 count(distinct f) over (partition by index order by f rows between 1 following and 2 following),
                 count(distinct f) over (partition by index order by f rows between unbounded preceding and 1 following) from windowing_distinct;

-- sum
select index, f, sum(distinct f) over (partition by index order by f rows between 2 preceding and 1 preceding),
                 sum(distinct f) over (partition by index order by f rows between unbounded preceding and 1 preceding),
                 sum(distinct f) over (partition by index order by f rows between 1 following and 2 following),
                 sum(distinct f) over (partition by index order by f rows between unbounded preceding and 1 following) from windowing_distinct;

-- avg
select index, f, avg(distinct f) over (partition by index order by f rows between 2 preceding and 1 preceding),
                 avg(distinct f) over (partition by index order by f rows between unbounded preceding and 1 preceding),
                 avg(distinct f) over (partition by index order by f rows between 1 following and 2 following),
                 avg(distinct f) over (partition by index order by f rows between unbounded preceding and 1 following) from windowing_distinct;
