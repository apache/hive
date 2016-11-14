--Restriction.1.h SubQueries only supported in the SQL Where Clause.
select src.key in (select key from src s1 where s1.key > '9') 
from src;

select count(*) 
from src 
group by src.key in (select key from src s1 where s1.key > '9') ;

--Restriction.2.h The subquery can only be the RHS of an expression
----curently paser doesn't allow such queries
--select * from part where (select p_size from part) IN (1,2);

--Restriction.3.m The predicate operators supported are In, Not In, exists and Not exists.
----select * from part where p_brand > (select key from src);  

--Check.4.h For Exists and Not Exists, the Sub Query must have 1 or more correlated predicates.
select * from src where exists (select * from part);

--Check.5.h multiple columns in subquery select
select * from src where src.key in (select * from src s1 where s1.key > '9');

--Restriction.6.m The LHS in a SubQuery must have all its Column References be qualified
--This is not restriction anymore

--Restriction 7.h subquery with or condition
select count(*) 
from src 
where src.key in (select key from src s1 where s1.key > '9') or src.value is not null
;

--Restriction.8.m We allow only 1 SubQuery expression per Query
select * from part where p_size IN (select p_size from part) AND p_brand IN (select p_brand from part);

--Restriction 9.m nested subquery
select *
from part x 
where x.p_name in (select y.p_name from part y where exists (select z.p_name from part z where y.p_name = z.p_name))
;

--Restriction.10.h In a SubQuery references to Parent Query columns is only supported in the where clause.
select * from part where p_size in (select p.p_size + part.p_size from part p);
select * from part where part.p_size IN (select min(p_size) from part p group by part.p_type);


--Restriction.11.m A SubQuery predicate that refers to a Parent Query column must be a valid Join predicate
select * from part where p_size in (select p_size from part p where p.p_type > part.p_type);
select * from part where part.p_size IN (select min(p_size) from part p where NOT(part.p_type = p.p_type));


--Check.12.h SubQuery predicates cannot only refer to Parent Query columns
select * from part where p_name IN (select p_name from part p where part.p_type <> 1);

--Restriction.13.m In the case of an implied Group By on a correlated Sub- Query, the SubQuery always returns 1 row. For e.g. a count on an empty set is 0, while all other UDAFs return null. Converting such a SubQuery into a Join by computing all Groups in one shot, changes the semantics: the Group By SubQuery output will not contain rows for Groups that don’t exist.
select * 
from src b 
where exists 
  (select count(*) 
  from src a 
  where b.value = a.value  and a.key = b.key and a.value > 'val_9'
  )
;

--Restriction.14.h Correlated Sub Queries cannot contain Windowing clauses.
select p_mfgr, p_name, p_size 
from part a 
where a.p_size in 
  (select first_value(p_size) over(partition by p_mfgr order by p_size) 
   from part b 
   where a.p_brand = b.p_brand)
;

--Restriction 15.h all unqualified column references in a SubQuery will resolve to table sources within the SubQuery.
select *
from src
where src.key in (select key from src where key > '9')
;

----------------------------------------------------------------
-- Following tests does not fall under any restrictions per-se, they just currently don't work with HIVE
----------------------------------------------------------------

-- correlated var which refers to outer query join table 
explain select p.p_partkey, li.l_suppkey from (select distinct l_partkey as p_partkey from lineitem) p join lineitem li on p.p_partkey = li.l_partkey where li.l_linenumber = 1 and li.l_orderkey in (select l_orderkey from lineitem where l_shipmode = 'AIR' and l_partkey = p.l_partkey) ;

-- union, not in, corr
explain select * from part where p_name NOT IN (select p_name from part p where p.p_mfgr = part.p_comment UNION ALL select p_brand from part);

-- union, not in, corr, cor var in both queries
explain select * from part where p_name NOT IN (select p_name from part p where p.p_mfgr = part.p_comment UNION ALL select p_brand from part pp where pp.p_mfgr = part.p_comment);

-- IN, union, corr
explain select * from part where p_name IN (select p_name from part p where p.p_mfgr = part.p_comment UNION ALL select p_brand from part);
