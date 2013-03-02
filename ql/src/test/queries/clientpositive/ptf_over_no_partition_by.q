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

-- 1. single aggregate

select p_name, p_retailprice,
avg(p_retailprice) over()
from part
order by p_name;

-- 2. multiple aggregates

select p_name, p_retailprice,
lead(p_retailprice) as l1 over(),
lag(p_retailprice) as l2 over()
from part
order by p_name;
