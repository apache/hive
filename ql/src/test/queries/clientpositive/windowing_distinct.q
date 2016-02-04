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
           dec decimal,
           bin binary)
       row format delimited
       fields terminated by '|';

load data local inpath '../../data/files/windowing_distinct.txt' into table windowing_distinct;


SELECT COUNT(DISTINCT t) OVER (PARTITION BY index),
       COUNT(DISTINCT d) OVER (PARTITION BY index),
       COUNT(DISTINCT bo) OVER (PARTITION BY index),
       COUNT(DISTINCT s) OVER (PARTITION BY index),
       COUNT(DISTINCT concat('Mr.', s)) OVER (PARTITION BY index),
       COUNT(DISTINCT ts) OVER (PARTITION BY index),
       COUNT(DISTINCT dec) OVER (PARTITION BY index),
       COUNT(DISTINCT bin) OVER (PARTITION BY index)
FROM windowing_distinct;

SELECT SUM(DISTINCT t) OVER (PARTITION BY index),
       SUM(DISTINCT d) OVER (PARTITION BY index),
       SUM(DISTINCT s) OVER (PARTITION BY index),
       SUM(DISTINCT concat('Mr.', s)) OVER (PARTITION BY index),
       SUM(DISTINCT ts) OVER (PARTITION BY index),
       SUM(DISTINCT dec) OVER (PARTITION BY index)
FROM windowing_distinct;

SELECT AVG(DISTINCT t) OVER (PARTITION BY index),
       AVG(DISTINCT d) OVER (PARTITION BY index),
       AVG(DISTINCT s) OVER (PARTITION BY index),
       AVG(DISTINCT concat('Mr.', s)) OVER (PARTITION BY index),
       AVG(DISTINCT ts) OVER (PARTITION BY index),
       AVG(DISTINCT dec) OVER (PARTITION BY index)
FROM windowing_distinct;
