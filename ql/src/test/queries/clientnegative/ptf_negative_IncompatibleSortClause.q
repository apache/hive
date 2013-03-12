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

-- testIncompatibleSortClause 
select  p_mfgr,p_name, p_size,  
rank() over(distribute by p_mfgr) as r, dense_rank() over(distribute by p_mfgr) as dr,  
sum(p_size) over (w1)   as s
from part    
window w1 as (partition by p_mfgr order by p_name  rows between 2 preceding and 2 following);
