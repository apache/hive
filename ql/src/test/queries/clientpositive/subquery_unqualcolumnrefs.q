--! qt:dataset:src
--! qt:dataset:part
set hive.mapred.mode=nonstrict;
create table src11_n0 (key1 string, value1 string);

create table part2_n2( 
    p2_partkey INT,
    p2_name STRING,
    p2_mfgr STRING,
    p2_brand STRING,
    p2_type STRING,
    p2_size INT,
    p2_container STRING,
    p2_retailprice DOUBLE,
    p2_comment STRING
);

-- non agg, corr
explain select * from src11_n0 where src11_n0.key1 in (select key from src where src11_n0.value1 = value and key > '9');

explain select * from src a where a.key in (select key from src where a.value = value and key > '9');

-- distinct, corr
explain 
select * 
from src b 
where b.key in
        (select distinct key 
         from src 
         where b.value = value and key > '9'
        )
;

explain
select p_mfgr, b.p_name, p_size 
from part b 
where b.p_name not in 
  (select p_name 
  from (select p_mfgr, p_name, p_size, rank() over(partition by p_mfgr order by p_size) as r from part) a 
  where r <= 2 and b.p_mfgr = p_mfgr 
  )
;