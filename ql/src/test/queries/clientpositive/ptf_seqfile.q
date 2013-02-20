DROP TABLE part_seq;

CREATE TABLE part_seq( 
    p_partkey INT,
    p_name STRING,
    p_mfgr STRING,
    p_brand STRING,
    p_type STRING,
    p_size INT,
    p_container STRING,
    p_retailprice DOUBLE,
    p_comment STRING
) STORED AS SEQUENCEFILE ;

LOAD DATA LOCAL INPATH '../data/files/part.seq' overwrite into table part_seq;

-- testWindowingPTFWithPartSeqFile
select p_mfgr, p_name, p_size, 
rank() as r, 
dense_rank() as dr, 
sum(p_retailprice) as s1 over (distribute by p_mfgr sort by p_name rows between unbounded preceding and current row) 
from noop(part_seq 
distribute by p_mfgr 
sort by p_name);  
