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

-- 1. testWindowing
select p_mfgr, p_name, p_size,
rank() as r,
dense_rank() as dr,
sum(p_retailprice) as s1 over (rows between unbounded preceding and current row)
from part
distribute by p_mfgr
sort by p_name;

-- 2. testGroupByWithPartitioning
select p_mfgr, p_name, p_size, min(p_retailprice),
rank() as r,
dense_rank() as dr,
p_size, p_size - lag(p_size,1) as deltaSz
from part
group by p_mfgr, p_name, p_size
distribute by p_mfgr
sort by p_name ;
       
-- 3. testGroupByHavingWithSWQ
select p_mfgr, p_name, p_size, min(p_retailprice),
rank() as r,
dense_rank() as dr,
p_size, p_size - lag(p_size,1) as deltaSz
from part
group by p_mfgr, p_name, p_size
having p_size > 0
distribute by p_mfgr
sort by p_name ;

-- 4. testCount
select p_mfgr, p_name, 
count(p_size) as cd 
from part 
distribute by p_mfgr 
sort by p_name;

-- 5. testCountWithWindowingUDAF
select p_mfgr, p_name, 
rank() as r, 
dense_rank() as dr, 
count(p_size) as cd, 
p_retailprice, sum(p_retailprice) as s1 over (rows between unbounded preceding and current row), 
p_size, p_size - lag(p_size,1) as deltaSz 
from part 
distribute by p_mfgr 
sort by p_name;

-- 6. testCountInSubQ
select sub1.r, sub1.dr, sub1.cd, sub1.s1, sub1.deltaSz 
from (select p_mfgr, p_name, 
rank() as r, 
dense_rank() as dr, 
count(p_size) as cd, 
p_retailprice, sum(p_retailprice) as s1 over (rows between unbounded preceding and current row), 
p_size, p_size - lag(p_size,1) as deltaSz 
from part 
distribute by p_mfgr 
sort by p_name
) sub1;

-- 7. testJoinWithWindowingAndPTF
select abc.p_mfgr, abc.p_name, 
rank() as r, 
dense_rank() as dr, 
abc.p_retailprice, sum(abc.p_retailprice) as s1 over (rows between unbounded preceding and current row), 
abc.p_size, abc.p_size - lag(abc.p_size,1) as deltaSz 
from noop(on part 
partition by p_mfgr 
order by p_name 
) abc join part p1 on abc.p_partkey = p1.p_partkey 
distribute by abc.p_mfgr 
sort by abc.p_name ;

-- 8. testMixedCaseAlias
select p_mfgr, p_name, p_size, rank() as R
from part 
distribute by p_mfgr 
sort by p_name, p_size desc;

-- 9. testHavingWithWindowingNoGBY
select p_mfgr, p_name, p_size, 
rank() as r, 
dense_rank() as dr, 
sum(p_retailprice) as s1 over (rows between unbounded preceding and current row) 
from part 
having p_size > 5 
distribute by p_mfgr 
sort by p_name; 

-- 10. testHavingWithWindowingCondRankNoGBY
select p_mfgr, p_name, p_size, 
rank() as r, 
dense_rank() as dr, 
sum(p_retailprice) as s1 over (rows between unbounded preceding and current row) 
from part 
having rank() < 4 
distribute by p_mfgr 
sort by p_name;

-- 11. testFirstLast   
select  p_mfgr,p_name, p_size, 
sum(p_size) as s2 over (rows between current row and current row), 
first_value(p_size) as f over w1 , 
last_value(p_size, false) as l over w1  
from part 
distribute by p_mfgr 
sort by p_mfgr 
window w1 as (rows between 2 preceding and 2 following);

-- 12. testFirstLastWithWhere
select  p_mfgr,p_name, p_size, 
rank() as r, 
sum(p_size) as s2 over (rows between current row and current row), 
first_value(p_size) as f over w1,  
last_value(p_size, false) as l over w1 
from part 
where p_mfgr = 'Manufacturer#3' 
distribute by p_mfgr 
sort by p_mfgr 
window w1 as (rows between 2 preceding and 2 following);

-- 13. testSumWindow
select  p_mfgr,p_name, p_size,  
sum(p_size) as s1 over w1, 
sum(p_size) as s2 over (rows between current row and current row) 
from part 
distribute by p_mfgr 
sort by p_mfgr 
window w1 as (rows between 2 preceding and 2 following);

-- 14. testNoSortClause
select  p_mfgr,p_name, p_size, 
rank() as r, dense_rank() as dr 
from part 
distribute by p_mfgr 
window w1 as (rows between 2 preceding and 2 following);

-- 15. testExpressions
select  p_mfgr,p_name, p_size,  
rank() as r,  
dense_rank() as dr, 
cume_dist() as cud, 
percent_rank() as pr, 
ntile(3) as nt, 
count(p_size) as ca, 
avg(p_size) as avg, 
stddev(p_size) as st, 
first_value(p_size % 5) as fv, 
last_value(p_size) as lv, 
first_value(p_size, true) as fvW1 over w1 
from part 
having p_size > 5 
distribute by p_mfgr 
sort by p_mfgr, p_name 
window w1 as (rows between 2 preceding and 2 following);

-- 16. testMultipleWindows
select  p_mfgr,p_name, p_size,  
  rank() as r, dense_rank() as dr, 
cume_dist() as cud, 
sum(p_size) as s1 over (rows between unbounded preceding and current row), 
sum(p_size) as s2 over (range between p_size 5 less and current row), 
first_value(p_size, true) as fv1 over w1 
from part 
having p_size > 5 
distribute by p_mfgr 
sort by p_mfgr, p_name 
window w1 as (rows between 2 preceding and 2 following);

-- 17. testCountStar
select  p_mfgr,p_name, p_size,
count(*) as c, 
count(p_size) as ca, 
first_value(p_size, true) as fvW1 over w1 
from part 
having p_size > 5 
distribute by p_mfgr 
sort by p_mfgr, p_name 
window w1 as (rows between 2 preceding and 2 following);

-- 18. testUDAFs
select  p_mfgr,p_name, p_size, 
sum(p_retailprice) as s over w1, 
min(p_retailprice) as mi over w1,
max(p_retailprice) as ma over w1,
avg(p_retailprice) as ag over w1
from part
distribute by p_mfgr
sort by p_mfgr, p_name
window w1 as (rows between 2 preceding and 2 following);

-- 19. testUDAFsWithGBY
select  p_mfgr,p_name, p_size, p_retailprice, 
sum(p_retailprice) as s over w1, 
min(p_retailprice) as mi ,
max(p_retailprice) as ma ,
avg(p_retailprice) as ag over w1
from part
group by p_mfgr,p_name, p_size, p_retailprice
distribute by p_mfgr
sort by p_mfgr, p_name
window w1 as (rows between 2 preceding and 2 following);

-- 20. testSTATs
select  p_mfgr,p_name, p_size, 
stddev(p_retailprice) as sdev over w1, 
stddev_pop(p_retailprice) as sdev_pop over w1, 
collect_set(p_size) as uniq_size over w1, 
variance(p_retailprice) as var over w1,
corr(p_size, p_retailprice) as cor over w1,
covar_pop(p_size, p_retailprice) as covarp over w1
from part
distribute by p_mfgr
sort by p_mfgr, p_name
window w1 as (rows between 2 preceding and 2 following);

-- 21. testDISTs
select  p_mfgr,p_name, p_size, 
histogram_numeric(p_retailprice, 5) as hist over w1, 
percentile(p_partkey, 0.5) as per over w1,
row_number() as rn
from part
distribute by p_mfgr
sort by p_mfgr, p_name
window w1 as (rows between 2 preceding and 2 following);

-- 22. testViewAsTableInputWithWindowing
create view IF NOT EXISTS mfgr_price_view as 
select p_mfgr, p_brand, 
sum(p_retailprice) as s 
from part 
group by p_mfgr, p_brand;
        
select p_mfgr, p_brand, s, 
sum(s) as s1 over w1 
from mfgr_price_view 
distribute by p_mfgr 
sort by p_mfgr 
window w1 as (rows between 2 preceding and current row);

-- 23. testCreateViewWithWindowingQuery
create view IF NOT EXISTS mfgr_brand_price_view as 
select p_mfgr, p_brand, 
sum(p_retailprice) as s over w1 
from part 
distribute by p_mfgr 
sort by p_mfgr 
window w1 as (rows between 2 preceding and current row);
        
select * from mfgr_brand_price_view;        
        
-- 24. testLateralViews
select p_mfgr, p_name, 
lv_col, p_size, sum(p_size) as s over w1  
from (select p_mfgr, p_name, p_size, array(1,2,3) arr from part) p 
lateral view explode(arr) part_lv as lv_col
distribute by p_mfgr 
sort by p_name 
window w1 as (rows between 2 preceding and current row);        

-- 25. testMultipleInserts3SWQs
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
dense_rank() as dr, 
sum(p_retailprice) as s over (rows between unbounded preceding and current row) 
distribute by p_mfgr 
sort by p_name  
INSERT OVERWRITE TABLE part_2 
select  p_mfgr,p_name, p_size,  
rank() as r, dense_rank() as dr, 
cume_dist() as cud, 
sum(p_size) as s1 over (rows between unbounded preceding and current row), 
sum(p_size) as s2 over (range between p_size 5 less and current row), 
first_value(p_size, true) as fv1 over w1 
having p_size > 5 
distribute by p_mfgr 
sort by p_mfgr, p_name 
window w1 as (rows between 2 preceding and 2 following) 
INSERT OVERWRITE TABLE part_3 
select  p_mfgr,p_name, p_size,  
count(*) as c, 
count(p_size) as ca, 
first_value(p_size, true) as fv over w1 
having p_size > 5 
distribute by p_mfgr 
sort by p_mfgr, p_name 
window w1 as (rows between 2 preceding and 2 following);

select * from part_1;

select * from part_2;

select * from part_3;

-- 26. testGroupByHavingWithSWQAndAlias
select p_mfgr, p_name, p_size, min(p_retailprice) as mi,
rank() as r,
dense_rank() as dr,
p_size, p_size - lag(p_size,1) as deltaSz
from part
group by p_mfgr, p_name, p_size
having p_size > 0
distribute by p_mfgr
sort by p_name;
	 
-- 27. testMultipleRangeWindows
select  p_mfgr,p_name, p_size, 
sum(p_size) as s2 over (range between p_size 10 less and current row), 
sum(p_size) as s1 over (range between current row and p_size 10 more ) 
from part 
distribute by p_mfgr 
sort by p_mfgr, p_size 
window w1 as (rows between 2 preceding and 2 following);

-- 28. testPartOrderInUDAFInvoke
select p_mfgr, p_name, p_size,
sum(p_size) as s over (partition by p_mfgr  order by p_name  rows between 2 preceding and 2 following)
from part;

-- 29. testPartOrderInWdwDef
select p_mfgr, p_name, p_size,
sum(p_size) as s over w1
from part
window w1 as (partition by p_mfgr  order by p_name  rows between 2 preceding and 2 following);

-- 30. testDefaultPartitioningSpecRules
select p_mfgr, p_name, p_size,
sum(p_size) as s over w1,
 sum(p_size) as s2 over w2
from part
sort by p_name
window w1 as (partition by p_mfgr rows between 2 preceding and 2 following),
       w2 as (partition by p_mfgr order by p_name);
       
-- 31. testWindowCrossReference
select p_mfgr, p_name, p_size, 
sum(p_size) as s1 over w1, 
sum(p_size) as s2 over w2
from part 
window w1 as (partition by p_mfgr order by p_mfgr rows between 2 preceding and 2 following), 
       w2 as w1;
       
               
-- 32. testWindowInheritance
select p_mfgr, p_name, p_size, 
sum(p_size) as s1 over w1, 
sum(p_size) as s2 over w2 
from part 
window w1 as (partition by p_mfgr order by p_mfgr rows between 2 preceding and 2 following), 
       w2 as (w1 rows between unbounded preceding and current row); 

        
-- 33. testWindowForwardReference
select p_mfgr, p_name, p_size, 
sum(p_size) as s1 over w1, 
sum(p_size) as s2 over w2,
sum(p_size) as s3 over w3 
from part 
distribute by p_mfgr 
sort by p_mfgr 
window w1 as (rows between 2 preceding and 2 following), 
       w2 as w3,
       w3 as (rows between unbounded preceding and current row); 


-- 34. testWindowDefinitionPropagation
select p_mfgr, p_name, p_size, 
sum(p_size) as s1 over w1, 
sum(p_size) as s2 over w2,
sum(p_size) as s3 over (w3 rows between 2 preceding and 2 following) 
from part 
distribute by p_mfgr 
sort by p_mfgr 
window w1 as (rows between 2 preceding and 2 following), 
       w2 as w3,
       w3 as (rows between unbounded preceding and current row); 

-- 35. testDistinctWithWindowing
select DISTINCT p_mfgr, p_name, p_size,
sum(p_size) as s over w1
from part
distribute by p_mfgr
sort by p_name
window w1 as (rows between 2 preceding and 2 following);

-- 36. testRankWithPartitioning
select p_mfgr, p_name, p_size, 
rank() as r over (partition by p_mfgr order by p_name ) 
from part;    

-- 37. testPartitioningVariousForms
select p_mfgr, p_name, p_size,
sum(p_retailprice) as s1 over (partition by p_mfgr order by p_mfgr),
min(p_retailprice) as s2 over (partition by p_mfgr),
max(p_retailprice) as s3 over (distribute by p_mfgr sort by p_mfgr),
avg(p_retailprice) as s4 over (distribute by p_mfgr),
count(p_retailprice) as s5 over (cluster by p_mfgr )
from part;

-- 38. testPartitioningVariousForms2
select p_mfgr, p_name, p_size,
sum(p_retailprice) as s1 over (partition by p_mfgr, p_name order by p_mfgr, p_name rows between unbounded preceding and current row),
min(p_retailprice) as s2 over (distribute by p_mfgr, p_name sort by p_mfgr, p_name rows between unbounded preceding and current row),
max(p_retailprice) as s3 over (cluster by p_mfgr, p_name )
from part;

-- 39. testUDFOnOrderCols
select p_mfgr, p_type, substr(p_type, 2) as short_ptype,
rank() as r over (partition by p_mfgr order by substr(p_type, 2)) 
from part;

-- 40. testNoBetweenForRows
select p_mfgr, p_name, p_size,
    sum(p_retailprice) as s1 over (rows unbounded preceding)
     from part distribute by p_mfgr sort by p_name;

-- 41. testNoBetweenForRange
select p_mfgr, p_name, p_size,
    sum(p_retailprice) as s1 over (range unbounded preceding)
     from part distribute by p_mfgr sort by p_name;

-- 42. testUnboundedFollowingForRows
select p_mfgr, p_name, p_size,
    sum(p_retailprice) as s1 over (rows between current row and unbounded following)
    from part distribute by p_mfgr sort by p_name;

-- 43. testUnboundedFollowingForRange
select p_mfgr, p_name, p_size,
    sum(p_retailprice) as s1 over (range between current row and unbounded following)
    from part distribute by p_mfgr sort by p_name;
        
-- 44. testOverNoPartitionSingleAggregate
select p_name, p_retailprice,
avg(p_retailprice) over()
from part
order by p_name;
        