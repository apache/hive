set hive.mapred.mode=nonstrict;
set hive.explain.user=false;

-- SORT_QUERY_RESULTS

-- non agg, non corr
explain
 select * 
from src 
where src.key in (select key from src s1 where s1.key > '9')
;

select * 
from src 
where src.key in (select key from src s1 where s1.key > '9')
;

-- non agg, corr
explain 
select * 
from src b 
where b.key in
        (select a.key 
         from src a 
         where b.value = a.value and a.key > '9'
        )
;

select * 
from src b 
where b.key in
        (select a.key 
         from src a 
         where b.value = a.value and a.key > '9'
        )
;


-- agg, non corr
explain
select p_name, p_size 
from 
part where part.p_size in 
	(select avg(p_size) 
	 from (select p_size, rank() over(partition by p_mfgr order by p_size) as r from part) a 
	 where r <= 2
	)
;
select p_name, p_size 
from 
part where part.p_size in 
	(select avg(p_size) 
	 from (select p_size, rank() over(partition by p_mfgr order by p_size) as r from part) a 
	 where r <= 2
	)
;

-- agg, corr
explain
select p_mfgr, p_name, p_size
from part b where b.p_size in
	(select min(p_size)
	 from (select p_mfgr, p_size, rank() over(partition by p_mfgr order by p_size) as r from part) a
	 where r <= 2 and b.p_mfgr = a.p_mfgr
	)
;

select p_mfgr, p_name, p_size
from part b where b.p_size in
	(select min(p_size)
	 from (select p_mfgr, p_size, rank() over(partition by p_mfgr order by p_size) as r from part) a
	 where r <= 2 and b.p_mfgr = a.p_mfgr
	)
;

-- distinct, corr
explain 
select * 
from src b 
where b.key in
        (select distinct a.key 
         from src a 
         where b.value = a.value and a.key > '9'
        )
;

select * 
from src b 
where b.key in
        (select distinct a.key 
         from src a 
         where b.value = a.value and a.key > '9'
        )
;

-- non agg, non corr, windowing
select p_mfgr, p_name, p_size 
from part 
where part.p_size in 
  (select first_value(p_size) over(partition by p_mfgr order by p_size) from part)
;

-- non agg, non corr, with join in Parent Query
explain
select p.p_partkey, li.l_suppkey 
from (select distinct l_partkey as p_partkey from lineitem) p join lineitem li on p.p_partkey = li.l_partkey 
where li.l_linenumber = 1 and
 li.l_orderkey in (select l_orderkey from lineitem where l_shipmode = 'AIR')
;

select p.p_partkey, li.l_suppkey 
from (select distinct l_partkey as p_partkey from lineitem) p join lineitem li on p.p_partkey = li.l_partkey 
where li.l_linenumber = 1 and
 li.l_orderkey in (select l_orderkey from lineitem where l_shipmode = 'AIR')
;

-- non agg, corr, with join in Parent Query
select p.p_partkey, li.l_suppkey 
from (select distinct l_partkey as p_partkey from lineitem) p join lineitem li on p.p_partkey = li.l_partkey 
where li.l_linenumber = 1 and
 li.l_orderkey in (select l_orderkey from lineitem where l_shipmode = 'AIR' and l_linenumber = li.l_linenumber)
;

-- corr, agg in outer and inner
explain select sum(l_extendedprice) from lineitem, part where p_partkey = l_partkey and l_quantity IN (select avg(l_quantity) from lineitem where l_partkey = p_partkey);
select sum(l_extendedprice) from lineitem, part where p_partkey = l_partkey and l_quantity IN (select avg(l_quantity) from lineitem where l_partkey = p_partkey);


--where has multiple conjuction
explain select * from part where p_brand <> 'Brand#14' AND p_size IN (select (p_size) from part p where p.p_type = part.p_type group by p_size) AND p_size <> 340;
select * from part where p_brand <> 'Brand#14' AND p_size IN (select (p_size) from part p where p.p_type = part.p_type group by p_size) AND p_size <> 340;

--lhs contains non-simple expression
explain select * from part  where (p_size-1) IN (select min(p_size) from part group by p_type);
select * from part  where (p_size-1) IN (select min(p_size) from part group by p_type);

explain select * from part where (p_partkey*p_size) IN (select min(p_partkey) from part group by p_type);
select * from part where (p_partkey*p_size) IN (select min(p_partkey) from part group by p_type);

--lhs contains non-simple expression, corr
explain select count(*) as c from part as e where p_size + 100 IN (select p_partkey from part where p_name = e.p_name);
select count(*) as c from part as e where p_size + 100 IN (select p_partkey from part where p_name = e.p_name);

-- lhs contains udf expression
explain select * from part  where floor(p_retailprice) IN (select floor(min(p_retailprice)) from part group by p_type);
select * from part  where floor(p_retailprice) IN (select floor(min(p_retailprice)) from part group by p_type);

explain select * from part where p_name IN (select p_name from part p where p.p_size = part.p_size AND part.p_size + 121150 = p.p_partkey );
select * from part where p_name IN (select p_name from part p where p.p_size = part.p_size AND part.p_size + 121150 = p.p_partkey );

-- correlated query, multiple correlated variables referring to different outer var
explain select * from part where p_name IN (select p_name from part p where p.p_size = part.p_size AND part.p_partkey= p.p_partkey );
select * from part where p_name IN (select p_name from part p where p.p_size = part.p_size AND part.p_partkey= p.p_partkey );

-- correlated var refers to outer table alias
explain select p_name from (select p_name, p_type, p_brand as brand from part) fpart where fpart.p_type IN (select p_type from part where part.p_brand = fpart.brand);
select p_name from (select p_name, p_type, p_brand as brand from part) fpart where fpart.p_type IN (select p_type from part where part.p_brand = fpart.brand);
 
-- correlated var refers to outer table alias which is an expression 
explain select p_name from (select p_name, p_type, p_size+1 as size from part) fpart where fpart.p_type IN (select p_type from part where (part.p_size+1) = fpart.size);
select p_name from (select p_name, p_type, p_size+1 as size from part) fpart where fpart.p_type IN (select p_type from part where (part.p_size+1) = fpart.size);

-- where plus having
explain select key, count(*) from src where value IN (select value from src) group by key having count(*) in (select count(*) from src s1 where s1.key = '90' group by s1.key );
select key, count(*) from src where value IN (select value from src) group by key having count(*) in (select count(*) from src s1 where s1.key = '90' group by s1.key );

-- where with having, correlated
explain select key, count(*) from src where value IN (select value from src sc where sc.key = src.key ) group by key having count(*) in (select count(*) from src s1 where s1.key = '90' group by s1.key );
select key, count(*) from src where value IN (select value from src sc where sc.key = src.key ) group by key having count(*) in (select count(*) from src s1 where s1.key = '90' group by s1.key );

-- subquery with order by
explain select * from part  where (p_size-1) IN (select min(p_size) from part group by p_type) order by p_brand;
select * from part  where (p_size-1) IN (select min(p_size) from part group by p_type) order by p_brand;

--order by with limit
explain select * from part  where (p_size-1) IN (select min(p_size) from part group by p_type) order by p_brand limit 4;
select * from part  where (p_size-1) IN (select min(p_size) from part group by p_type) order by p_brand limit 4;

-- union, uncorr
explain select * from src where key IN (select p_name from part UNION ALL select p_brand from part);
select * from src where key IN (select p_name from part UNION ALL select p_brand from part);

-- corr, subquery has another subquery in from
explain select p_mfgr, b.p_name, p_size from part b where b.p_name in 
  (select p_name from (select p_mfgr, p_name, p_size as r from part) a where r < 10 and b.p_mfgr = a.p_mfgr ) order by p_mfgr,p_size;
select p_mfgr, b.p_name, p_size from part b where b.p_name in 
  (select p_name from (select p_mfgr, p_name, p_size as r from part) a where r < 10 and b.p_mfgr = a.p_mfgr ) order by p_mfgr,p_size;

-- join in subquery, correlated predicate with only one table
explain select p_partkey from part where p_name in (select p.p_name from part p left outer join part pp on p.p_type = pp.p_type where pp.p_size = part.p_size);
select p_partkey from part where p_name in (select p.p_name from part p left outer join part pp on p.p_type = pp.p_type where pp.p_size = part.p_size);

-- join in subquery, correlated predicate with both inner tables, same outer var
explain select p_partkey from part where p_name in 
	(select p.p_name from part p left outer join part pp on p.p_type = pp.p_type where pp.p_size = part.p_size and p.p_size=part.p_size);
select p_partkey from part where p_name in 
	(select p.p_name from part p left outer join part pp on p.p_type = pp.p_type where pp.p_size = part.p_size and p.p_size=part.p_size);

-- join in subquery, correlated predicate with both inner tables, different outer var
explain select p_partkey from part where p_name in 
	(select p.p_name from part p left outer join part pp on p.p_type = pp.p_type where pp.p_size = part.p_size and p.p_type=part.p_type);

-- subquery within from 
explain select p_partkey from 
	(select p_size, p_partkey from part where p_name in (select p.p_name from part p left outer join part pp on p.p_type = pp.p_type where pp.p_size = part.p_size)) subq;
select p_partkey from 
	(select p_size, p_partkey from part where p_name in (select p.p_name from part p left outer join part pp on p.p_type = pp.p_type where pp.p_size = part.p_size)) subq;

-- corr IN with COUNT aggregate
explain select * from part where p_size IN (select count(*) from part pp where pp.p_type = part.p_type);
select * from part where p_size IN (select count(*) from part pp where pp.p_type = part.p_type);

-- corr IN with aggregate other than COUNT
explain select * from part where p_size in (select avg(pp.p_size) from part pp where pp.p_partkey = part.p_partkey);
select * from part where p_size in (select avg(pp.p_size) from part pp where pp.p_partkey = part.p_partkey);

-- corr IN with aggregate other than COUNT (MIN) with non-equi join
explain select * from part where p_size in (select min(pp.p_size) from part pp where pp.p_partkey > part.p_partkey);
select * from part where p_size in (select min(pp.p_size) from part pp where pp.p_partkey > part.p_partkey);

-- corr IN with COUNT aggregate
explain select * from part where p_size NOT IN (select count(*) from part pp where pp.p_type = part.p_type);
select * from part where p_size NOT IN (select count(*) from part pp where pp.p_type = part.p_type);

-- corr IN with aggregate other than COUNT
explain select * from part where p_size not in (select avg(pp.p_size) from part pp where pp.p_partkey = part.p_partkey);
select * from part where p_size not in (select avg(pp.p_size) from part pp where pp.p_partkey = part.p_partkey);

create table t(i int);
insert into t values(1);
insert into t values(0);

create table tempty(i int);

-- uncorr sub with aggregate which produces result irrespective of zero rows
explain select * from t where i IN (select count(*) from tempty);
select * from t where i IN (select count(*) from tempty);

drop table t;

create table tnull(i int);
insert into tnull values(NULL) , (NULL);

-- empty inner table, non-null sq key, expected empty result
select * from part where p_size IN (select i from tempty);

-- empty inner table, null sq key, expected empty result
select * from tnull where i IN (select i from tempty);

-- null inner table, non-null sq key
select * from part where p_size IN (select i from tnull);

-- null inner table, null sq key
select * from tnull where i IN (select i from tnull);

drop table tempty;

create table t(i int, j int);
insert into t values(0,1), (0,2);

create table tt(i int, j int);
insert into tt values(0,3);

-- corr IN with aggregate other than COUNT return zero rows
explain select * from t where i IN (select sum(i) from tt where tt.j = t.j);
select * from t where i IN (select sum(i) from tt where tt.j = t.j);

drop table t;
drop table tt;
