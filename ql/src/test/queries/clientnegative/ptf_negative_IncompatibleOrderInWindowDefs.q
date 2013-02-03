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

-- testIncompatibleOrderInWindowDefs
select p_mfgr, p_name, p_size, 
sum(p_size) as s1 over (w1), 
sum(p_size) as s2 over (w2) 
from part 
distribute by p_mfgr 
sort by p_mfgr 
window w1 as distribute by p_mfgr sort by p_mfgr rows between 2 preceding and 2 following, 
       w2 as distribute by p_mfgr sort by p_name rows between unbounded preceding and current row; 
