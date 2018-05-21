set hive.mapred.mode=nonstrict;
set hive.optimize.skewjoin.compiletime = true;
set hive.auto.convert.join=true;
    
CREATE TABLE T1_n157(key STRING, val STRING)	
SKEWED BY (key) ON ((2)) STORED AS TEXTFILE;	
       
LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n157;	
     
CREATE TABLE T2_n92(key STRING, val STRING) STORED AS TEXTFILE;	
       
LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2_n92;	
     
-- copy from skewjoinopt11
-- test compile time skew join and auto map join	 
-- This test is to verify the skew join compile optimization when the join is followed
-- by a union. Both sides of a union consist of a join, which should have used
-- skew join compile time optimization.
-- adding an order by at the end to make the results deterministic

EXPLAIN	 
select * from	
(      
  select a.key, a.val as val1, b.val as val2 from T1_n157 a join T2_n92 b on a.key = b.key
    union all 	
  select a.key, a.val as val1, b.val as val2 from T1_n157 a join T2_n92 b on a.key = b.key
) subq1; 
  
select * from	
(      
  select a.key, a.val as val1, b.val as val2 from T1_n157 a join T2_n92 b on a.key = b.key
    union all 	
  select a.key, a.val as val1, b.val as val2 from T1_n157 a join T2_n92 b on a.key = b.key
) subq1
ORDER BY key, val1, val2;
