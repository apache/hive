set hive.cbo.enable=false;

drop table over10k_n14;

create table over10k_n14(
           t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           `dec` decimal(4,2),
           bin binary)
       row format delimited
       fields terminated by '|';

load data local inpath '../../data/files/over10k' into table over10k_n14;

 EXPLAIN
  SELECT fv
    FROM (SELECT distinct first_value(t) OVER ( PARTITION BY si ORDER BY i ) AS fv
            FROM over10k_n14) sq
ORDER BY fv
   LIMIT 10;

  SELECT fv
    FROM (SELECT distinct first_value(t) OVER ( PARTITION BY si ORDER BY i ) AS fv
            FROM over10k_n14) sq
ORDER BY fv 
   LIMIT 10;

 EXPLAIN
  SELECT lv
    FROM (SELECT distinct last_value(i) OVER ( PARTITION BY si ORDER BY i ) AS lv
            FROM over10k_n14) sq
ORDER BY lv
   LIMIT 10;

  SELECT lv
    FROM (SELECT distinct last_value(i) OVER ( PARTITION BY si ORDER BY i ) AS lv
            FROM over10k_n14) sq
ORDER BY lv
   LIMIT 10;

 EXPLAIN
  SELECT lv, fv
    FROM (SELECT distinct last_value(i) OVER ( PARTITION BY si ORDER BY i ) AS lv,
                          first_value(t) OVER ( PARTITION BY si ORDER BY i ) AS fv
            FROM over10k_n14) sq
ORDER BY lv, fv
   LIMIT 50;

  SELECT lv, fv 
    FROM (SELECT distinct last_value(i) OVER ( PARTITION BY si ORDER BY i ) AS lv,
                          first_value(t) OVER ( PARTITION BY si ORDER BY i ) AS fv
            FROM over10k_n14) sq
ORDER BY lv, fv
   LIMIT 50;

explain
select si, max(f) mf, rank() over ( partition by si order by mf ) r
FROM over10k_n14
GROUP BY si
HAVING max(f) > 0
ORDER BY si, r
limit 50;

select si, max(f) mf, rank() over ( partition by si order by mf ) r
FROM over10k_n14
GROUP BY si
HAVING max(f) > 0
ORDER BY si, r
limit 50;

explain
select distinct si, rank() over ( partition by si order by i ) r
FROM over10k_n14
ORDER BY si, r
limit 50;

select distinct si, rank() over ( partition by si order by i ) r
FROM over10k_n14
ORDER BY si, r
limit 50;
