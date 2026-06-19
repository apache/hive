set hive.mapred.mode=nonstrict;
set hive.optimize.skewjoin.compiletime = true;

CREATE TABLE T1_n65(key STRING, val STRING)
SKEWED BY (key) ON ((2)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n65;

CREATE TABLE T2_n39(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2_n39;

CREATE TABLE T3_n14(key STRING, val STRING)
SKEWED BY (val) ON ((12)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T3.txt' INTO TABLE T3_n14;

-- This test is for skewed join compile time optimization for more than 2 tables.
-- The join key for table 3 is different from the join key used for joining
-- tables 1 and 2. Tables 1 and 3 are skewed. Since one of the join sources for table
-- 3 consist of a sub-query which contains a join, the compile time skew join 
-- optimization is not enabled for table 3, but it is used for the first join between
-- tables 1 and 2
-- adding a order by at the end to make the results deterministic

EXPLAIN
select *
from 
T1_n65 a join T2_n39 b on a.key = b.key 
join T3_n14 c on a.val = c.val;

select *
from 
T1_n65 a join T2_n39 b on a.key = b.key 
join T3_n14 c on a.val = c.val
order by a.key, b.key, a.val, b.val;

