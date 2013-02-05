DROP TABLE part;
DROP TABLE flights_tiny;

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

create table flights_tiny ( 
ORIGIN_CITY_NAME string, 
DEST_CITY_NAME string, 
YEAR int, 
MONTH int, 
DAY_OF_MONTH int, 
ARR_DELAY float, 
FL_NUM string 
);

LOAD DATA LOCAL INPATH '../data/files/flights_tiny.txt' OVERWRITE INTO TABLE flights_tiny;

--1. test1
select p_mfgr, p_name, p_size,
rank() as r,
denserank() as dr,
sum(p_retailprice) as s1 over (rows between unbounded preceding and current row)
from noop(part 
  distribute by p_mfgr
  sort by p_name
  );

-- 2. test1NoPTF
select p_mfgr, p_name, p_size,
rank() as r,
denserank() as dr,
sum(p_retailprice) as s1 over (rows between unbounded preceding and current row)
from part
distribute by p_mfgr
sort by p_name;

--3. testLeadLag
select p_mfgr, p_name,
rank() as r,
denserank() as dr,
p_retailprice, sum(p_retailprice) as s1 over (rows between unbounded preceding and current row),
p_size, p_size - lag(p_size,1) as deltaSz
from noop(part
distribute by p_mfgr
sort by p_name 
);

-- 4. testLeadLagNoPTF
select p_mfgr, p_name,
rank() as r,
denserank() as dr,
p_retailprice, sum(p_retailprice) as s1 over (rows between unbounded preceding and current row),
p_size, p_size - lag(p_size,1) as deltaSz
from part
distribute by p_mfgr
sort by p_name ;   

-- 5. testLeadLagNoPTFNoWindowing
select p_mfgr, p_name, p_size
from part
distribute by p_mfgr
sort by p_name ;

-- 6. testJoinWithLeadLag
select p1.p_mfgr, p1.p_name,
p1.p_size, p1.p_size - lag(p1.p_size,1) as deltaSz
from part p1 join part p2 on p1.p_partkey = p2.p_partkey
distribute by p1.p_mfgr
sort by p1.p_name ;
        
-- 7. testJoinWithNoop
select p_mfgr, p_name,
p_size, p_size - lag(p_size,1) as deltaSz
from noop ( (select p1.* from part p1 join part p2 on p1.p_partkey = p2.p_partkey) j
distribute by j.p_mfgr
sort by j.p_name);    
        
-- 8. testGroupByWithSWQ
select p_mfgr, p_name, p_size, min(p_retailprice),
rank() as r,
denserank() as dr,
p_size, p_size - lag(p_size,1) as deltaSz
from part
group by p_mfgr, p_name, p_size
distribute by p_mfgr
sort by p_name ;
        
-- 9. testGroupByHavingWithSWQ
select p_mfgr, p_name, p_size, min(p_retailprice),
rank() as r,
denserank() as dr,
p_size, p_size - lag(p_size,1) as deltaSz
from part
group by p_mfgr, p_name, p_size
having p_size > 0
distribute by p_mfgr
sort by p_name ;
        
-- 10. testOnlyPTF
select p_mfgr, p_name, p_size
from noop(part
distribute by p_mfgr
sort by p_name);

-- 11. testAlias
select p_mfgr, p_name, p_size,
rank() as r,
denserank() as dr,
sum(p_retailprice) as s1 over (rows between unbounded preceding and current row)
from noop(part 
  distribute by p_mfgr
  sort by p_name
  ) abc;
  
-- 12. testSWQAndPTFAndWhere
select p_mfgr, p_name, p_size, 
rank() as r, 
denserank() as dr, 
p_size, p_size - lag(p_size,1) as deltaSz 
from noop(part 
          distribute by p_mfgr 
          sort by p_name 
          ) 
where p_size > 0 
distribute by p_mfgr 
sort by p_name;

-- 13. testSWQAndPTFAndGBy
select p_mfgr, p_name, p_size, 
rank() as r, 
denserank() as dr, 
p_size, p_size - lag(p_size,1) as deltaSz 
from noop(part 
          distribute by p_mfgr 
          sort by p_name 
          ) 
group by p_mfgr, p_name, p_size  
distribute by p_mfgr 
sort by p_name;

-- 14. testCountNoWindowing
select p_mfgr, p_name, 
rank() as r, 
denserank() as dr, 
count(p_size) as cd 
from noop(part 
distribute by p_mfgr 
sort by p_name);

-- 15. testCountWithWindowing
select p_mfgr, p_name, 
rank() as r, 
denserank() as dr, 
count(p_size) as cd, 
p_retailprice, sum(p_retailprice) as s1 over (rows between unbounded preceding and current row), 
p_size, p_size - lag(p_size,1) as deltaSz 
from noop(part 
distribute by p_mfgr 
sort by p_name);


-- 16. testCountInSubQ
select sub1.r, sub1.dr, sub1.cd, sub1.s1, sub1.deltaSz 
from (select p_mfgr, p_name, 
rank() as r, 
denserank() as dr, 
count(p_size) as cd, 
p_retailprice, sum(p_retailprice) as s1 over (rows between unbounded preceding and current row), 
p_size, p_size - lag(p_size,1) as deltaSz 
from noop(part 
distribute by p_mfgr 
sort by p_name)
) sub1;

-- 17. testJoin
select abc.* 
from noop(part 
distribute by p_mfgr 
sort by p_name 
) abc join part p1 on abc.p_partkey = p1.p_partkey;

-- 18. testJoinRight
select abc.* 
from part p1 join noop(part 
distribute by p_mfgr 
sort by p_name 
) abc on abc.p_partkey = p1.p_partkey;

-- 19. testJoinWithWindowing
select abc.p_mfgr, abc.p_name, 
rank() as r, 
denserank() as dr, 
abc.p_retailprice, sum(abc.p_retailprice) as s1 over (rows between unbounded preceding and current row), 
abc.p_size, abc.p_size - lag(abc.p_size,1) as deltaSz 
from noop(part 
distribute by p_mfgr 
sort by p_name 
) abc join part p1 on abc.p_partkey = p1.p_partkey 
distribute by abc.p_mfgr 
sort by abc.p_name ;

-- 20. testMixedCaseAlias
select p_mfgr, p_name, p_size, rank() as R
from noop(part 
distribute by p_mfgr 
sort by p_name, p_size desc);

-- 21. testNoopWithMap
select p_mfgr, p_name, p_size, rank() as r
from noopwithmap(part
distribute by p_mfgr
sort by p_name, p_size desc);

-- 22. testNoopWithMapWithWindowing 
select p_mfgr, p_name, p_size,
rank() as r,
denserank() as dr,
sum(p_retailprice) as s1 over (rows between unbounded preceding and current row)
from noopwithmap(part 
  distribute by p_mfgr
  sort by p_name);

-- 23. testHavingWithWindowingNoGBY
select p_mfgr, p_name, p_size, 
rank() as r, 
denserank() as dr, 
sum(p_retailprice) as s1 over (rows between unbounded preceding and current row) 
from part 
having p_size > 5 
distribute by p_mfgr 
sort by p_name; 
    		
-- 24. testHavingWithWindowingCondRankNoGBY
select p_mfgr, p_name, p_size, 
rank() as r, 
denserank() as dr, 
sum(p_retailprice) as s1 over (rows between unbounded preceding and current row) 
from part 
having r < 4 
distribute by p_mfgr 
sort by p_name;

-- 25. testHavingWithWindowingPTFNoGBY
select p_mfgr, p_name, p_size,
rank() as r,
denserank() as dr,
sum(p_retailprice) as s1 over (rows between unbounded preceding and current row)
from noop(part
distribute by p_mfgr
sort by p_name)
having r < 4;

-- 26. testFirstLast   
select  p_mfgr,p_name, p_size, 
sum(p_size) as s2 over (rows between current row and current row), 
first_value(p_size) as f over (w1) , 
last_value(p_size, false) as l over (w1)  
from part 
distribute by p_mfgr 
sort by p_mfgr 
window w1 as rows between 2 preceding and 2 following;

-- 27. testFirstLastWithWhere
select  p_mfgr,p_name, p_size, 
rank() as r, 
sum(p_size) as s2 over (rows between current row and current row), 
first_value(p_size) as f over (w1),  
last_value(p_size, false) as l over (w1) 
from part 
where p_mfgr = 'Manufacturer#3' 
distribute by p_mfgr 
sort by p_mfgr 
window w1 as rows between 2 preceding and 2 following;

-- 28. testSumDelta
select  p_mfgr,p_name, p_size,   
sum(p_size - lag(p_size,1)) as deltaSum over (w1)
from part 
distribute by p_mfgr 
sort by p_mfgr 
window w1 as rows between 2 preceding and 2 following ;

-- 29. testSumWindow
select  p_mfgr,p_name, p_size,  
sum(p_size) as s1 over (w1), 
sum(p_size) as s2 over (rows between current row and current row) 
from part 
distribute by p_mfgr 
sort by p_mfgr 
window w1 as rows between 2 preceding and 2 following;

-- 30. testNoSortClause
select  p_mfgr,p_name, p_size, 
rank() as r, denserank() as dr 
from part 
distribute by p_mfgr 
window w1 as rows between 2 preceding and 2 following;

-- 31. testExpressions
select  p_mfgr,p_name, p_size,  
rank() as r,  
denserank() as dr, 
cumedist() as cud, 
percentrank() as pr, 
ntile(3) as nt, 
count(p_size) as ca, 
avg(p_size) as avg, 
stddev(p_size) as st, 
first_value(p_size % 5) as fv, 
last_value(p_size) as lv, 
first_value(p_size, true) as fvW1 over (w1) 
from part 
having p_size > 5 
distribute by p_mfgr 
sort by p_mfgr, p_name 
window w1 as rows between 2 preceding and 2 following;

-- 32. testMultipleWindows
select  p_mfgr,p_name, p_size,  
  rank() as r, denserank() as dr, 
cumedist() as cud, 
sum(p_size) as s1 over (rows between unbounded preceding and current row), 
sum(p_size) as s2 over (range between p_size 5 less and current row), 
first_value(p_size, true) as fv1 over (w1) 
from part 
having p_size > 5 
distribute by p_mfgr 
sort by p_mfgr, p_name 
window w1 as rows between 2 preceding and 2 following;

-- 33. testFunctionChain
select p_mfgr, p_name, p_size, 
rank() as r, 
denserank() as dr, 
sum(p_retailprice) as s1 over (rows between unbounded preceding and current row) 
from noop(noopwithmap(noop(part 
distribute by p_mfgr 
sort by p_mfgr, p_name
)));
 
-- 34. testPTFAndWindowingInSubQ
select p_mfgr, p_name, 
sub1.cd, sub1.s1 
from (select p_mfgr, p_name, 
count(p_size) as cd, 
p_retailprice, 
sum(p_retailprice) as s1 over (w1) 
from noop(part 
distribute by p_mfgr 
sort by p_name) 
window w1 as rows between 2 preceding and 2 following 
) sub1 ;

-- 35. testCountStar
select  p_mfgr,p_name, p_size,
count(*) as c, 
count(p_size) as ca, 
first_value(p_size, true) as fvW1 over (w1) 
from part 
having p_size > 5 
distribute by p_mfgr 
sort by p_mfgr, p_name 
window w1 as rows between 2 preceding and 2 following;

-- 36. testJoinWithWindowingWithCount
select abc.p_mfgr, abc.p_name, 
rank() as r, 
denserank() as dr, 
count(abc.p_name) as cd, 
abc.p_retailprice, sum(abc.p_retailprice) as s1 over (rows between unbounded preceding and current row), 
abc.p_size, abc.p_size - lag(abc.p_size,1) as deltaSz 
from noop(part 
distribute by p_mfgr 
sort by p_name 
) abc join part p1 on abc.p_partkey = p1.p_partkey 
distribute by abc.p_mfgr 
sort by abc.p_name;

-- 37. testSumDelta
select  p_mfgr,p_name, p_size,   
sum(p_size - lag(p_size,1)) as deltaSum 
from part 
distribute by p_mfgr 
sort by p_mfgr 
window w1 as rows between 2 preceding and 2 following ;

-- 38. testUDAFs
select  p_mfgr,p_name, p_size, 
sum(p_retailprice) as s over (w1), 
min(p_retailprice) as mi over (w1),
max(p_retailprice) as ma over (w1),
avg(p_retailprice) as ag over (w1)
from part
distribute by p_mfgr
sort by p_mfgr, p_name
window w1 as rows between 2 preceding and 2 following;

-- 39. testUDAFsWithPTFWithGBY
select  p_mfgr,p_name, p_size, p_retailprice, 
sum(p_retailprice) as s over (w1), 
min(p_retailprice) as mi ,
max(p_retailprice) as ma ,
avg(p_retailprice) as ag over (w1)
from part
group by p_mfgr,p_name, p_size, p_retailprice
distribute by p_mfgr
sort by p_mfgr, p_name
window w1 as rows between 2 preceding and 2 following;

-- 40. testSTATs
select  p_mfgr,p_name, p_size, 
stddev(p_retailprice) as sdev over (w1), 
stddev_pop(p_retailprice) as sdev_pop over (w1), 
collect_set(p_size) as uniq_size over (w1), 
variance(p_retailprice) as var over (w1),
corr(p_size, p_retailprice) as cor over (w1),
covar_pop(p_size, p_retailprice) as covarp over (w1)
from part
distribute by p_mfgr
sort by p_mfgr, p_name
window w1 as rows between 2 preceding and 2 following;

-- 41. testDISTs
select  p_mfgr,p_name, p_size, 
histogram_numeric(p_retailprice, 5) as hist over (w1), 
percentile(p_partkey, 0.5) as per over (w1),
rownumber() as rn
from part
distribute by p_mfgr
sort by p_mfgr, p_name
window w1 as rows between 2 preceding and 2 following;

-- 42. testDistinctInSelectWithPTF
select DISTINCT p_mfgr, p_name, p_size 
from noop(part 
distribute by p_mfgr 
sort by p_name);
        
-- 43. testUDAFsNoWindowingNoPTFNoGBY
select p_mfgr,p_name, p_retailprice,  
sum(p_retailprice) as s,
min(p_retailprice) as mi,
max(p_retailprice) as ma,
avg(p_retailprice) as av 
from part 
distribute by p_mfgr 
sort by p_mfgr, p_name;        

-- 44. testViewAsTableInputWithWindowing
create view IF NOT EXISTS mfgr_price_view as 
select p_mfgr, p_brand, 
sum(p_retailprice) as s 
from part 
group by p_mfgr, p_brand;
        
select p_mfgr, p_brand, s, 
sum(s) as s1 over (w1) 
from mfgr_price_view 
distribute by p_mfgr 
sort by p_mfgr 
window w1 as rows between 2 preceding and current row;

-- 45. testViewAsTableInputToPTF
select p_mfgr, p_brand, s, 
sum(s) as s1 over (w1) 
from noop(mfgr_price_view 
distribute by p_mfgr 
sort by p_mfgr)  
window w1 as rows between 2 preceding and current row;

-- 46. testCreateViewWithWindowingQuery
create view IF NOT EXISTS mfgr_brand_price_view as 
select p_mfgr, p_brand, 
sum(p_retailprice) as s over (w1) 
from part 
distribute by p_mfgr 
sort by p_mfgr 
window w1 as rows between 2 preceding and current row;
        
select * from mfgr_brand_price_view;        
        
-- 47. testLateralViews
select p_mfgr, p_name, 
lv_col, p_size, sum(p_size) as s over (w1)  
from (select p_mfgr, p_name, p_size, array(1,2,3) arr from part) p 
lateral view explode(arr) part_lv as lv_col
distribute by p_mfgr 
sort by p_name 
window w1 as rows between 2 preceding and current row;        
        
-- 48. testConstExprInSelect
select 'tst1' as key, count(1) as value from part;

-- 49. testMultipleInserts3SWQs
CREATE TABLE part_1( 
p_mfgr STRING, 
p_name STRING, 
p_size INT, 
r INT, 
dr INT, 
s DOUBLE);

CREATE TABLE part_2( 
p_mfgr STRING, 
p_name STRING, 
p_size INT, 
r INT, 
dr INT, 
cud INT, 
s1 DOUBLE, 
s2 DOUBLE, 
fv1 INT);

CREATE TABLE part_3( 
p_mfgr STRING, 
p_name STRING, 
p_size INT, 
c INT, 
ca INT, 
fv INT);

from part 
INSERT OVERWRITE TABLE part_1 
select p_mfgr, p_name, p_size, 
rank() as r, 
denserank() as dr, 
sum(p_retailprice) as s over (rows between unbounded preceding and current row) 
distribute by p_mfgr 
sort by p_name  
INSERT OVERWRITE TABLE part_2 
select  p_mfgr,p_name, p_size,  
rank() as r, denserank() as dr, 
cumedist() as cud, 
sum(p_size) as s1 over (rows between unbounded preceding and current row), 
sum(p_size) as s2 over (range between p_size 5 less and current row), 
first_value(p_size, true) as fv1 over (w1) 
having p_size > 5 
distribute by p_mfgr 
sort by p_mfgr, p_name 
window w1 as rows between 2 preceding and 2 following 
INSERT OVERWRITE TABLE part_3 
select  p_mfgr,p_name, p_size,  
count(*) as c, 
count(p_size) as ca, 
first_value(p_size, true) as fv over (w1) 
having p_size > 5 
distribute by p_mfgr 
sort by p_mfgr, p_name 
window w1 as rows between 2 preceding and 2 following;

select * from part_1;

select * from part_2;

select * from part_3;
	 
-- 50. testGroupByHavingWithSWQAndAlias
select p_mfgr, p_name, p_size, min(p_retailprice) as mi,
rank() as r,
denserank() as dr,
p_size, p_size - lag(p_size,1) as deltaSz
from part
group by p_mfgr, p_name, p_size
having p_size > 0
distribute by p_mfgr
sort by p_name;

-- 51. testMultipleRangeWindows
select  p_mfgr,p_name, p_size, 
sum(p_size) as s2 over (range between p_size 10 less and current row), 
sum(p_size) as s1 over (range between current row and p_size 10 more ) 
from part 
distribute by p_mfgr 
sort by p_mfgr, p_size 
window w1 as rows between 2 preceding and 2 following;

-- 52. testMultipleInserts2SWQsWithPTF
CREATE TABLE part_4( 
p_mfgr STRING, 
p_name STRING, 
p_size INT, 
r INT, 
dr INT, 
s DOUBLE);

CREATE TABLE part_5( 
p_mfgr STRING, 
p_name STRING, 
p_size INT, 
s1 INT, 
s2 INT, 
r INT, 
dr INT, 
cud DOUBLE, 
fv1 INT);

from noop(part 
distribute by p_mfgr 
sort by p_name) 
INSERT OVERWRITE TABLE part_4 select p_mfgr, p_name, p_size, 
rank() as r, 
denserank() as dr, 
sum(p_retailprice) as s over (rows between unbounded preceding and current row) 
distribute by p_mfgr 
sort by p_name  
INSERT OVERWRITE TABLE part_5 select  p_mfgr,p_name, p_size,  
sum(p_size) as s1 over (rows between unbounded preceding and current row), 
sum(p_size) as s2 over (range between p_size 5 less and current row), 
rank() as r, 
denserank() as dr, 
cumedist() as cud, 
first_value(p_size, true) as fv1 over (w1) 
having p_size > 5 
distribute by p_mfgr 
sort by p_mfgr, p_name 
window w1 as rows between 2 preceding and 2 following;

select * from part_4;

select * from part_5;

-- 53. testPartOrderInUDAFInvoke
select p_mfgr, p_name, p_size,
sum(p_size) as s over (distribute by p_mfgr  sort by p_name  rows between 2 preceding and 2 following)
from part;

-- 54. testPartOrderInWdwDef
select p_mfgr, p_name, p_size,
sum(p_size) as s over (w1)
from part
window w1 as distribute by p_mfgr  sort by p_name  rows between 2 preceding and 2 following;

-- 55. testDefaultPartitioningSpecRules
select p_mfgr, p_name, p_size,
sum(p_size) as s over (w1),
 sum(p_size) as s2 over(w2)
from part
sort by p_name
window w1 as distribute by p_mfgr rows between 2 preceding and 2 following,
       w2 as distribute by p_mfgr sort by p_name;
       
-- 56. testWindowCrossReference
select p_mfgr, p_name, p_size, 
sum(p_size) as s1 over (w1), 
sum(p_size) as s2 over (w2)
from part 
window w1 as distribute by p_mfgr sort by p_mfgr rows between 2 preceding and 2 following, 
       w2 as w1;
       
               
-- 57. testWindowInheritance
select p_mfgr, p_name, p_size, 
sum(p_size) as s1 over (w1), 
sum(p_size) as s2 over (w2) 
from part 
window w1 as distribute by p_mfgr sort by p_mfgr rows between 2 preceding and 2 following, 
       w2 as w1 rows between unbounded preceding and current row; 

        
-- 58. testWindowForwardReference
select p_mfgr, p_name, p_size, 
sum(p_size) as s1 over (w1), 
sum(p_size) as s2 over (w2),
sum(p_size) as s3 over (w3) 
from part 
distribute by p_mfgr 
sort by p_mfgr 
window w1 as rows between 2 preceding and 2 following, 
       w2 as w3,
       w3 as rows between unbounded preceding and current row; 


-- 59. testWindowDefinitionPropagation
select p_mfgr, p_name, p_size, 
sum(p_size) as s1 over (w1), 
sum(p_size) as s2 over (w2),
sum(p_size) as s3 over (w3 rows between 2 preceding and 2 following) 
from part 
distribute by p_mfgr 
sort by p_mfgr 
window w1 as rows between 2 preceding and 2 following, 
       w2 as w3,
       w3 as rows between unbounded preceding and current row; 

-- 60. testDistinctWithWindowing
select DISTINCT p_mfgr, p_name, p_size,
sum(p_size) as s over (w1)
from part
distribute by p_mfgr
sort by p_name
window w1 as rows between 2 preceding and 2 following;

-- 61. testMulti2OperatorsFunctionChainWithMap
select p_mfgr, p_name,  
rank() as r, 
denserank() as dr, 
p_size, sum(p_size) as s1 over (rows between unbounded preceding and current row) 
from noop(
        noopwithmap(
          noop(
              noop(part 
              distribute by p_mfgr 
              sort by p_mfgr) 
            ) 
          distribute by p_mfgr,p_name 
          sort by p_mfgr,p_name) 
        distribute by p_mfgr,p_name  
        sort by p_mfgr,p_name) ;

-- 62. testMulti3OperatorsFunctionChain
select p_mfgr, p_name,  
rank() as r, 
denserank() as dr, 
p_size, sum(p_size) as s1 over (rows between unbounded preceding and current row) 
from noop(
        noop(
          noop(
              noop(part 
              distribute by p_mfgr 
              sort by p_mfgr) 
            ) 
          distribute by p_mfgr,p_name 
          sort by p_mfgr,p_name) 
        distribute by p_mfgr  
        sort by p_mfgr ) ;
        
-- 63. testMultiOperatorChainWithNoWindowing
select p_mfgr, p_name,  
rank() as r, 
denserank() as dr, 
p_size, sum(p_size) as s1 
from noop(
        noop(
          noop(
              noop(part 
              distribute by p_mfgr,p_name 
              sort by p_mfgr,p_name) 
            ) 
          distribute by p_mfgr 
          sort by p_mfgr)); 


-- 64. testMultiOperatorChainEndsWithNoopMap
select p_mfgr, p_name,  
rank() as r, 
denserank() as dr, 
p_size, sum(p_size) as s1 over (rows between unbounded preceding and current row)  
from noopwithmap(
        noop(
          noop(
              noop(part 
              distribute by p_mfgr,p_name 
              sort by p_mfgr,p_name) 
            ) 
          distribute by p_mfgr 
          sort by p_mfgr) 
          distribute by p_mfgr,p_name 
          sort by p_mfgr,p_name); 

--65. testMultiOperatorChainWithDiffPartitionForWindow1
select p_mfgr, p_name,  
rank() as r, 
denserank() as dr, 
p_size, 
sum(p_size) as s1 over (rows between unbounded preceding and current row), 
sum(p_size) as s2 over (distribute by p_mfgr,p_name sort by p_mfgr,p_name rows between unbounded preceding and current row) 
from noop(
        noopwithmap(
              noop(part 
              distribute by p_mfgr, p_name 
              sort by p_mfgr, p_name) 
          distribute by p_mfgr 
          sort by p_mfgr
          )); 

--66. testMultiOperatorChainWithDiffPartitionForWindow2
select p_mfgr, p_name,  
rank() as r, 
denserank() as dr, 
p_size, 
sum(p_size) as s1 over (rows between unbounded preceding and current row), 
sum(p_size) as s2 over (distribute by p_mfgr sort by p_mfgr rows between unbounded preceding and current row) 
from noopwithmap(
        noop(
              noop(part 
              distribute by p_mfgr, p_name 
              sort by p_mfgr, p_name) 
          ));

-- 67. basic Npath test
select origin_city_name, fl_num, year, month, day_of_month, sz, tpath 
from npath( 
      'LATE.LATE+', 
      'LATE', arr_delay > 15, 
    'origin_city_name, fl_num, year, month, day_of_month, size(tpath) as sz, tpath as tpath' 
     on 
        flights_tiny 
        distribute by fl_num 
      sort by year, month, day_of_month  
   );       
   
-- 68. testRankWithPartitioning
select p_mfgr, p_name, p_size, 
rank() as r over (distribute by p_mfgr sort by p_name ) 
from part;    
