DROP TABLE part;

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

-- testHavingLeadWithNoGBYNoWindowing
select  p_mfgr,p_name, p_size 
from part 
having lead(p_size, 1) <= p_size 
distribute by p_mfgr 
sort by p_name;
