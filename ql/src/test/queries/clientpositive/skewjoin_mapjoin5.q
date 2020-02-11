set hive.mapred.mode=nonstrict;
set hive.optimize.skewjoin.compiletime = true;
set hive.auto.convert.join=true;

CREATE TABLE T1_n87(key STRING, val STRING)
SKEWED BY (key) ON ((2)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n87;

CREATE TABLE T2_n54(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2_n54;

-- copy from skewjoinopt9
-- test compile time skew join and auto map join
-- no skew join compile time optimization would be performed if one of the
-- join sources is a sub-query consisting of a union all
-- adding a order by at the end to make the results deterministic
EXPLAIN
select * from
(
select key, val from T1_n87
  union all 
select key, val from T1_n87
) subq1
join T2_n54 b on subq1.key = b.key;

select * from
(
select key, val from T1_n87
  union all 
select key, val from T1_n87
) subq1
join T2_n54 b on subq1.key = b.key
ORDER BY subq1.key, b.key, subq1.val, b.val;

-- no skew join compile time optimization would be performed if one of the
-- join sources is a sub-query consisting of a group by
EXPLAIN
select * from
(
select key, count(1) as cnt from T1_n87 group by key
) subq1
join T2_n54 b on subq1.key = b.key;

select * from
(
select key, count(1) as cnt from T1_n87 group by key
) subq1
join T2_n54 b on subq1.key = b.key
ORDER BY subq1.key, b.key, subq1.cnt, b.val;
