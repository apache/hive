-- SORT_QUERY_RESULTS

--1. test1
explain
select p_mfgr, p_name, p_size,
rank() over (partition by p_mfgr order by p_name) as r,
dense_rank() over (partition by p_mfgr order by p_name) as dr,
sum(p_retailprice) over (partition by p_mfgr order by p_name rows between unbounded preceding and current row) as s1
from noopstreaming(on part 
  partition by p_mfgr
  order by p_name
  );

select p_mfgr, p_name, p_size,
rank() over (partition by p_mfgr order by p_name) as r,
dense_rank() over (partition by p_mfgr order by p_name) as dr,
sum(p_retailprice) over (partition by p_mfgr order by p_name rows between unbounded preceding and current row) as s1
from noopstreaming(on part 
  partition by p_mfgr
  order by p_name
  );
  
-- 2. testJoinWithNoop
explain
select p_mfgr, p_name,
p_size, p_size - lag(p_size,1,p_size) over (partition by p_mfgr order by p_name) as deltaSz
from noopstreaming (on (select p1.* from part p1 join part p2 on p1.p_partkey = p2.p_partkey) j
distribute by j.p_mfgr
sort by j.p_name)
;  

select p_mfgr, p_name,
p_size, p_size - lag(p_size,1,p_size) over (partition by p_mfgr order by p_name) as deltaSz
from noopstreaming (on (select p1.* from part p1 join part p2 on p1.p_partkey = p2.p_partkey) j
distribute by j.p_mfgr
sort by j.p_name)
;  

-- 7. testJoin
explain 
select abc.* 
from noopstreaming(on part 
partition by p_mfgr 
order by p_name 
) abc join part p1 on abc.p_partkey = p1.p_partkey;

select abc.* 
from noopstreaming(on part 
partition by p_mfgr 
order by p_name 
) abc join part p1 on abc.p_partkey = p1.p_partkey;

-- 9. testNoopWithMap
explain
select p_mfgr, p_name, p_size, 
rank() over (partition by p_mfgr order by p_name, p_size desc) as r
from noopwithmapstreaming(on part
partition by p_mfgr
order by p_name, p_size desc);

select p_mfgr, p_name, p_size, 
rank() over (partition by p_mfgr order by p_name, p_size desc) as r
from noopwithmapstreaming(on part
partition by p_mfgr
order by p_name, p_size desc);

-- 10. testNoopWithMapWithWindowing 
explain
select p_mfgr, p_name, p_size,
rank() over (partition by p_mfgr order by p_name) as r,
dense_rank() over (partition by p_mfgr order by p_name) as dr,
sum(p_retailprice) over (partition by p_mfgr order by p_name rows between unbounded preceding and current row) as s1
from noopwithmapstreaming(on part 
  partition by p_mfgr
  order by p_name);

select p_mfgr, p_name, p_size,
rank() over (partition by p_mfgr order by p_name) as r,
dense_rank() over (partition by p_mfgr order by p_name) as dr,
sum(p_retailprice) over (partition by p_mfgr order by p_name rows between unbounded preceding and current row) as s1
from noopwithmapstreaming(on part 
  partition by p_mfgr
  order by p_name);
  
-- 12. testFunctionChain
explain
select p_mfgr, p_name, p_size, 
rank() over (partition by p_mfgr order by p_name) as r, 
dense_rank() over (partition by p_mfgr order by p_name) as dr, 
sum(p_retailprice) over (partition by p_mfgr order by p_name rows between unbounded preceding and current row)  as s1
from noopstreaming(on noopwithmapstreaming(on noopstreaming(on part 
partition by p_mfgr 
order by p_mfgr, p_name
)));

select p_mfgr, p_name, p_size, 
rank() over (partition by p_mfgr order by p_name) as r, 
dense_rank() over (partition by p_mfgr order by p_name) as dr, 
sum(p_retailprice) over (partition by p_mfgr order by p_name rows between unbounded preceding and current row)  as s1
from noopstreaming(on noopwithmapstreaming(on noopstreaming(on part 
partition by p_mfgr 
order by p_mfgr, p_name
)));

-- 12.1 testFunctionChain
explain
select p_mfgr, p_name, p_size, 
rank() over (partition by p_mfgr order by p_name) as r, 
dense_rank() over (partition by p_mfgr order by p_name) as dr, 
sum(p_retailprice) over (partition by p_mfgr order by p_name rows between unbounded preceding and current row)  as s1
from noopstreaming(on noopwithmap(on noopstreaming(on part 
partition by p_mfgr 
order by p_mfgr, p_name
)));

select p_mfgr, p_name, p_size, 
rank() over (partition by p_mfgr order by p_name) as r, 
dense_rank() over (partition by p_mfgr order by p_name) as dr, 
sum(p_retailprice) over (partition by p_mfgr order by p_name rows between unbounded preceding and current row)  as s1
from noopstreaming(on noopwithmap(on noopstreaming(on part 
partition by p_mfgr 
order by p_mfgr, p_name
)));

-- 12.2 testFunctionChain
explain
select p_mfgr, p_name, p_size, 
rank() over (partition by p_mfgr order by p_name) as r, 
dense_rank() over (partition by p_mfgr order by p_name) as dr, 
sum(p_retailprice) over (partition by p_mfgr order by p_name rows between unbounded preceding and current row)  as s1
from noop(on noopwithmapstreaming(on noopstreaming(on part 
partition by p_mfgr 
order by p_mfgr, p_name
)));

select p_mfgr, p_name, p_size, 
rank() over (partition by p_mfgr order by p_name) as r, 
dense_rank() over (partition by p_mfgr order by p_name) as dr, 
sum(p_retailprice) over (partition by p_mfgr order by p_name rows between unbounded preceding and current row)  as s1
from noop(on noopwithmapstreaming(on noopstreaming(on part 
partition by p_mfgr 
order by p_mfgr, p_name
)));

-- 14. testPTFJoinWithWindowingWithCount
explain
select abc.p_mfgr, abc.p_name, 
rank() over (distribute by abc.p_mfgr sort by abc.p_name) as r, 
dense_rank() over (distribute by abc.p_mfgr sort by abc.p_name) as dr, 
count(abc.p_name) over (distribute by abc.p_mfgr sort by abc.p_name) as cd, 
abc.p_retailprice, sum(abc.p_retailprice) over (distribute by abc.p_mfgr sort by abc.p_name rows between unbounded preceding and current row) as s1, 
abc.p_size, abc.p_size - lag(abc.p_size,1,abc.p_size) over (distribute by abc.p_mfgr sort by abc.p_name) as deltaSz 
from noopstreaming(on part 
partition by p_mfgr 
order by p_name 
) abc join part p1 on abc.p_partkey = p1.p_partkey 
;

select abc.p_mfgr, abc.p_name, 
rank() over (distribute by abc.p_mfgr sort by abc.p_name) as r, 
dense_rank() over (distribute by abc.p_mfgr sort by abc.p_name) as dr, 
count(abc.p_name) over (distribute by abc.p_mfgr sort by abc.p_name) as cd, 
abc.p_retailprice, sum(abc.p_retailprice) over (distribute by abc.p_mfgr sort by abc.p_name rows between unbounded preceding and current row) as s1, 
abc.p_size, abc.p_size - lag(abc.p_size,1,abc.p_size) over (distribute by abc.p_mfgr sort by abc.p_name) as deltaSz 
from noopstreaming(on part 
partition by p_mfgr 
order by p_name 
) abc join part p1 on abc.p_partkey = p1.p_partkey 
;

-- 18. testMulti2OperatorsFunctionChainWithMap
explain
select p_mfgr, p_name,  
rank() over (partition by p_mfgr,p_name) as r, 
dense_rank() over (partition by p_mfgr,p_name) as dr, 
p_size, sum(p_size) over (partition by p_mfgr,p_name rows between unbounded preceding and current row)  as s1
from noopstreaming(on 
        noopwithmap(on 
          noop(on 
              noopstreaming(on part 
              partition by p_mfgr 
              order by p_mfgr) 
            ) 
          partition by p_mfgr,p_name 
          order by p_mfgr,p_name) 
        partition by p_mfgr,p_name  
        order by p_mfgr,p_name) ;

select p_mfgr, p_name,  
rank() over (partition by p_mfgr,p_name) as r, 
dense_rank() over (partition by p_mfgr,p_name) as dr, 
p_size, sum(p_size) over (partition by p_mfgr,p_name rows between unbounded preceding and current row)  as s1
from noopstreaming(on 
        noopwithmap(on 
          noop(on 
              noopstreaming(on part 
              partition by p_mfgr 
              order by p_mfgr) 
            ) 
          partition by p_mfgr,p_name 
          order by p_mfgr,p_name) 
        partition by p_mfgr,p_name  
        order by p_mfgr,p_name) ;
        
-- 19. testMulti3OperatorsFunctionChain
explain
select p_mfgr, p_name,  
rank() over (partition by p_mfgr order by p_name) as r, 
dense_rank() over (partition by p_mfgr order by p_name) as dr, 
p_size, sum(p_size) over (partition by p_mfgr order by p_name rows between unbounded preceding and current row)  as s1
from noop(on 
        noopstreaming(on 
          noop(on 
              noopstreaming(on part 
              partition by p_mfgr 
              order by p_mfgr) 
            ) 
          partition by p_mfgr,p_name 
          order by p_mfgr,p_name) 
        partition by p_mfgr  
        order by p_mfgr ) ;

select p_mfgr, p_name,  
rank() over (partition by p_mfgr order by p_name) as r, 
dense_rank() over (partition by p_mfgr order by p_name) as dr, 
p_size, sum(p_size) over (partition by p_mfgr order by p_name rows between unbounded preceding and current row)  as s1
from noop(on 
        noopstreaming(on 
          noop(on 
              noopstreaming(on part 
              partition by p_mfgr 
              order by p_mfgr) 
            ) 
          partition by p_mfgr,p_name 
          order by p_mfgr,p_name) 
        partition by p_mfgr  
        order by p_mfgr ) ;
        
-- 23. testMultiOperatorChainWithDiffPartitionForWindow2
explain
select p_mfgr, p_name,  
rank() over (partition by p_mfgr order by p_name) as r, 
dense_rank() over (partition by p_mfgr order by p_name) as dr, 
p_size, 
sum(p_size) over (partition by p_mfgr order by p_name range between unbounded preceding and current row) as s1, 
sum(p_size) over (partition by p_mfgr order by p_name range between unbounded preceding and current row)  as s2
from noopwithmapstreaming(on 
        noop(on 
              noopstreaming(on part 
              partition by p_mfgr, p_name 
              order by p_mfgr, p_name) 
          ));

select p_mfgr, p_name,  
rank() over (partition by p_mfgr order by p_name) as r, 
dense_rank() over (partition by p_mfgr order by p_name) as dr, 
p_size, 
sum(p_size) over (partition by p_mfgr order by p_name range between unbounded preceding and current row) as s1, 
sum(p_size) over (partition by p_mfgr order by p_name range between unbounded preceding and current row)  as s2
from noopwithmapstreaming(on 
        noop(on 
              noopstreaming(on part 
              partition by p_mfgr, p_name 
              order by p_mfgr, p_name) 
          ));
