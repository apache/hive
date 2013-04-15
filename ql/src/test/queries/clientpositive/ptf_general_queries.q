DROP TABLE part;

-- data setup
CREATE TABLE part( 
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

LOAD DATA LOCAL INPATH '../data/files/part_tiny.txt' overwrite into table part;

-- 1. testNoPTFNoWindowing
select p_mfgr, p_name, p_size
from part
distribute by p_mfgr
sort by p_name ;
        
-- 2. testUDAFsNoWindowingNoPTFNoGBY
select p_mfgr,p_name, p_retailprice,  
sum(p_retailprice) over(partition by p_mfgr order by p_mfgr) as s,
min(p_retailprice) over(partition by p_mfgr order by p_mfgr) as mi,
max(p_retailprice) over(partition by p_mfgr order by p_mfgr) as ma,
avg(p_retailprice) over(partition by p_mfgr order by p_mfgr) as av 
from part 
;        
        
-- 3. testConstExprInSelect
select 'tst1' as key, count(1) as value from part;
