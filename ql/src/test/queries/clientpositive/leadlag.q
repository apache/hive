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

--1. testLagWithPTFWindowing
select p_mfgr, p_name,
rank() as r,
dense_rank() as dr,
p_retailprice, sum(p_retailprice) as s1 over (partition by p_mfgr order by p_name rows between unbounded preceding and current row),
p_size, p_size - lag(p_size,1) as deltaSz
from noop(on part
partition by p_mfgr
order by p_name 
);

-- 2. testLagWithWindowingNoPTF
select p_mfgr, p_name,
rank() as r,
dense_rank() as dr,
p_retailprice, sum(p_retailprice) as s1 over (rows between unbounded preceding and current row),
p_size, p_size - lag(p_size,1) as deltaSz
from part
distribute by p_mfgr
sort by p_name ;   

-- 3. testJoinWithLag
select p1.p_mfgr, p1.p_name,
p1.p_size, p1.p_size - lag(p1.p_size,1) as deltaSz
from part p1 join part p2 on p1.p_partkey = p2.p_partkey
distribute by p1.p_mfgr
sort by p1.p_name ;

-- 4. testLagInSum
select  p_mfgr,p_name, p_size,   
sum(p_size - lag(p_size,1)) as deltaSum 
from part 
distribute by p_mfgr 
sort by p_mfgr 
window w1 as (rows between 2 preceding and 2 following) ;

-- 5. testLagInSumOverWindow
select  p_mfgr,p_name, p_size,   
sum(p_size - lag(p_size,1)) as deltaSum over w1
from part 
distribute by p_mfgr 
sort by p_mfgr 
window w1 as (rows between 2 preceding and 2 following) ;

-- 6. testRankInLead
select p_mfgr, p_name, p_size, 
rank() as r1, 
lead(rank(), 1) as deltaRank 
from part 
distribute by p_mfgr 
sort by p_name;

-- 7. testLeadWithPTF
select p_mfgr, p_name, 
rank() as r, 
dense_rank() as dr, 
p_size, p_size - lead(p_size,1) as deltaSz 
from noop(on part 
partition by p_mfgr 
order by p_name  
) 
distribute by p_mfgr 
sort by p_name;

-- 8. testOverNoPartitionMultipleAggregate
select p_name, p_retailprice,
lead(p_retailprice) as l1 over(),
lag(p_retailprice) as l2 over()
from part
order by p_name;
