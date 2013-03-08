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

-- 1. testLeadUDAF
select p_mfgr, p_retailprice,
lead(p_retailprice) as l1 over (partition by p_mfgr order by p_name),
lead(p_retailprice,1) as l2 over (partition by p_mfgr order by p_name),
lead(p_retailprice,1,10) as l3 over (partition by p_mfgr order by p_name),
lead(p_retailprice,1, p_retailprice) as l4 over (partition by p_mfgr order by p_name),
p_retailprice - lead(p_retailprice,1,p_retailprice)
from part;

-- 2.testLeadUDAFPartSz1
select p_mfgr, p_name, p_retailprice,
lead(p_retailprice,1) over (partition by p_mfgr, p_name ),
p_retailprice - lead(p_retailprice,1,p_retailprice)
from part;

-- 3.testLagUDAF
select p_mfgr, p_retailprice,
lag(p_retailprice,1) as l1 over (partition by p_mfgr order by p_name),
lag(p_retailprice) as l2 over (partition by p_mfgr order by p_name),
lag(p_retailprice,1, p_retailprice) as l3 over (partition by p_mfgr order by p_name),
lag(p_retailprice,1,10) as l4 over (partition by p_mfgr order by p_name),
p_retailprice - lag(p_retailprice,1,p_retailprice)
from part;

-- 4.testLagUDAFPartSz1
select p_mfgr, p_name, p_retailprice,
lag(p_retailprice,1) over (partition by p_mfgr, p_name ),
p_retailprice - lag(p_retailprice,1,p_retailprice)
from part;

-- 5.testLeadLagUDAF
select p_mfgr, p_retailprice,
lead(p_retailprice,1) as l1 over (partition by p_mfgr order by p_name),
lead(p_retailprice,1, p_retailprice) as l2 over (partition by p_mfgr order by p_name),
p_retailprice - lead(p_retailprice,1,p_retailprice),
lag(p_retailprice,1) as l3 over (partition by p_mfgr order by p_name),
lag(p_retailprice,1, p_retailprice) as l4 over (partition by p_mfgr order by p_name) 
from part;