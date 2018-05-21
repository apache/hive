--! qt:dataset:src
set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

-- data setup
DROP TABLE IF EXISTS part_subq;

CREATE TABLE part_subq( 
    p_partkey INT,
    p_name STRING,
    p_mfgr STRING,
    p_brand STRING,
    p_type STRING,
    p_size INT,
    p_container STRING,
    p_retailprice DOUBLE,
    p_comment STRING
);

LOAD DATA LOCAL INPATH '../../data/files/part_tiny.txt' overwrite into table part_subq;

-- non agg, non corr
explain
 select key, count(*) 
from src 
group by key
having count(*) in (select count(*) from src s1 where s1.key > '9' group by s1.key )
;


select s1.key, count(*) from src s1 where s1.key > '9' group by s1.key;

select key, count(*) 
from src 
group by key
having count(*) in (select count(*) from src s1 where s1.key = '90' group by s1.key )
;

set hive.optimize.correlation=false;

-- agg, non corr
explain
select p_mfgr, avg(p_size)
from part_subq b
group by b.p_mfgr
having b.p_mfgr in 
   (select p_mfgr 
    from part_subq
    group by p_mfgr
    having max(p_size) - min(p_size) < 20
   )
;

set hive.optimize.correlation=true;

-- agg, non corr
explain
select p_mfgr, avg(p_size)
from part_subq b
group by b.p_mfgr
having b.p_mfgr in
   (select p_mfgr
    from part_subq
    group by p_mfgr
    having max(p_size) - min(p_size) < 20
   )
;

-- join on agg
select b.key, min(b.value)
from src b
group by b.key
having b.key in ( select a.key
                from src a
                where a.value > 'val_9' and a.value = min(b.value)
                )
;

-- where and having
-- Plan is:
-- Stage 1: b semijoin sq1:src (subquery in where)
-- Stage 2: group by Stage 1 o/p
-- Stage 5: group by on sq2:src (subquery in having)
-- Stage 6: Stage 2 o/p semijoin Stage 5
explain
select key, value, count(*) 
from src b
where b.key in (select key from src where src.key > '8')
group by key, value
having count(*) in (select count(*) from src s1 where s1.key > '9' group by s1.key )
;
select key, value, count(*)
from src b
where b.key in (select key from src where src.key > '8')
group by key, value
having count(*) in (select count(*) from src s1 where s1.key > '9' group by s1.key )
;

set hive.auto.convert.join=true;
-- Plan is:
-- Stage  5: group by on sq2:src (subquery in having)
-- Stage 10: hashtable for sq1:src (subquery in where)
-- Stage  2: b map-side semijoin Stage 10 o/p
-- Stage  3: Stage 2 semijoin Stage 5
-- Stage  9: construct hastable for Stage 5 o/p
-- Stage  6: Stage 2 map-side semijoin Stage 9
explain
select key, value, count(*) 
from src b
where b.key in (select key from src where src.key > '8')
group by key, value
having count(*) in (select count(*) from src s1 where s1.key > '9' group by s1.key )
;

-- both having and where corr
explain
select key, value, count(*)
from src b
where b.key in (select key from src where src.value = b.value)
group by key, value
having count(*) in (select count(*) from src s1 where s1.key > '9' group by s1.key )
;
select key, value, count(*)
from src b
where b.key in (select key from src where src.value = b.value)
group by key, value
having count(*) in (select count(*) from src s1 where s1.key > '9' group by s1.key )
;

-- non agg, non corr, windowing
explain
select p_mfgr, p_name, avg(p_size) 
from part_subq 
group by p_mfgr, p_name
having p_name in 
  (select first_value(p_name) over(partition by p_mfgr order by p_size) from part_subq)
;

CREATE TABLE src_null_n4 (key STRING COMMENT 'default', value STRING COMMENT 'default') STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH "../../data/files/kv1.txt" INTO TABLE src_null_n4;
INSERT INTO src_null_n4 values('5444', null);

explain
select key, value, count(*)
from src_null_n4 b
where NOT EXISTS (select key from src_null_n4 where src_null_n4.value <> b.value)
group by key, value
having count(*) not in (select count(*) from src_null_n4 s1 where s1.key > '9' and s1.value <> b.value group by s1.key );

select key, value, count(*)
from src_null_n4 b
where NOT EXISTS (select key from src_null_n4 where src_null_n4.value <> b.value)
group by key, value
having count(*) not in (select count(*) from src_null_n4 s1 where s1.key > '9' and s1.value <> b.value group by s1.key );

DROP TABLE src_null_n4;
DROP TABLE part_subq;