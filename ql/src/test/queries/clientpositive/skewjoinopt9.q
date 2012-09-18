set hive.internal.ddl.list.bucketing.enable=true;
set hive.optimize.skewjoin.compiletime = true;

CREATE TABLE T1(key STRING, val STRING)
SKEWED BY (key) ON ((2)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../data/files/T1.txt' INTO TABLE T1;

CREATE TABLE T2(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../data/files/T2.txt' INTO TABLE T2;

-- no skew join compile time optimization would be performed if one of the
-- join sources is a sub-query consisting of a union all
EXPLAIN
select * from
(
select key, val from T1
  union all 
select key, val from T1
) subq1
join T2 b on subq1.key = b.key;

select * from
(
select key, val from T1
  union all 
select key, val from T1
) subq1
join T2 b on subq1.key = b.key;

-- no skew join compile time optimization would be performed if one of the
-- join sources is a sub-query consisting of a group by
EXPLAIN
select * from
(
select key, count(1) as cnt from T1 group by key
) subq1
join T2 b on subq1.key = b.key;

select * from
(
select key, count(1) as cnt from T1 group by key
) subq1
join T2 b on subq1.key = b.key;
