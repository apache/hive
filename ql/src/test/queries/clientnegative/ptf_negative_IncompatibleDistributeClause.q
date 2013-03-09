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

-- testIncompatibleDistributeClause 
select  p_mfgr,p_name, p_size,  
rank() as r, dense_rank() as dr,  
sum(p_size) over (w1) as s  
from part  
distribute by p_mfgr  
window w1 as (partition by p_name rows between 2 preceding and 2 following);
