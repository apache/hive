set hive.mapred.mode=nonstrict;

-- non agg, non corr
explain
select * 
from src 
where src.key not in  
  ( select key  from src s1 
    where s1.key > '2'
  )
;

select * 
from src 
where src.key not in  ( select key from src s1 where s1.key > '2')
order by key
;

-- non agg, corr
explain
select p_mfgr, b.p_name, p_size 
from part b 
where b.p_name not in 
  (select p_name 
  from (select p_mfgr, p_name, p_size, rank() over(partition by p_mfgr order by p_size) as r from part) a 
  where r <= 2 and b.p_mfgr = a.p_mfgr 
  )
;

select p_mfgr, b.p_name, p_size 
from part b 
where b.p_name not in 
  (select p_name 
  from (select p_mfgr, p_name, p_size, rank() over(partition by p_mfgr order by p_size) as r from part) a 
  where r <= 2 and b.p_mfgr = a.p_mfgr 
  )
order by p_mfgr, b.p_name
;

-- agg, non corr
explain
select p_name, p_size 
from 
part where part.p_size not in 
  (select avg(p_size) 
  from (select p_size, rank() over(partition by p_mfgr order by p_size) as r from part) a 
  where r <= 2
  )
;
select p_name, p_size 
from 
part where part.p_size not in 
  (select avg(p_size) 
  from (select p_size, rank() over(partition by p_mfgr order by p_size) as r from part) a 
  where r <= 2
  )
order by p_name, p_size
;

-- agg, corr
explain
select p_mfgr, p_name, p_size
from part b where b.p_size not in
  (select min(p_size)
  from (select p_mfgr, p_size, rank() over(partition by p_mfgr order by p_size) as r from part) a
  where r <= 2 and b.p_mfgr = a.p_mfgr
  )
;

select p_mfgr, p_name, p_size
from part b where b.p_size not in
  (select min(p_size)
  from (select p_mfgr, p_size, rank() over(partition by p_mfgr order by p_size) as r from part) a
  where r <= 2 and b.p_mfgr = a.p_mfgr
  )
;

-- non agg, non corr, Group By in Parent Query
select li.l_partkey, count(*)
from lineitem li
where li.l_linenumber = 1 and
  li.l_orderkey not in (select l_orderkey from lineitem where l_shipmode = 'AIR')
group by li.l_partkey
;

-- alternate not in syntax
select * 
from src 
where not src.key in  ( select key from src s1 where s1.key > '2')
order by key
;

-- null check
create view T1_v as 
select key from src where key <'11';

create view T2_v as 
select case when key > '104' then null else key end as key from T1_v;

explain
select * 
from T1_v where T1_v.key not in (select T2_v.key from T2_v);

select * 
from T1_v where T1_v.key not in (select T2_v.key from T2_v);

--where has multiple conjuction
explain select * from part where p_brand <> 'Brand#14' AND p_size NOT IN (select (p_size*p_size) from part p where p.p_type = part.p_type ) AND p_size <> 340;
select * from part where p_brand <> 'Brand#14' AND p_size NOT IN (select (p_size*p_size) from part p where p.p_type = part.p_type ) AND p_size <> 340;

--lhs contains non-simple expression
explain select * from part  where (p_size-1) NOT IN (select min(p_size) from part group by p_type) order by p_partkey;
select * from part  where (p_size-1) NOT IN (select min(p_size) from part group by p_type) order by p_partkey;

explain select * from part where (p_partkey*p_size) NOT IN (select min(p_partkey) from part group by p_type);
select * from part where (p_partkey*p_size) NOT IN (select min(p_partkey) from part group by p_type);

--lhs contains non-simple expression, corr
explain select count(*) as c from part as e where p_size + 100 NOT IN (select p_partkey from part where p_name = e.p_name);
select count(*) as c from part as e where p_size + 100 NOT IN (select p_partkey from part where p_name = e.p_name);

-- lhs contains udf expression
explain select * from part  where floor(p_retailprice) NOT IN (select floor(min(p_retailprice)) from part group by p_type);
select * from part  where floor(p_retailprice) NOT IN (select floor(min(p_retailprice)) from part group by p_type);

explain select * from part where p_name NOT IN (select p_name from part p where p.p_size = part.p_size AND part.p_size + 121150 = p.p_partkey );
select * from part where p_name NOT IN (select p_name from part p where p.p_size = part.p_size AND part.p_size + 121150 = p.p_partkey );

-- correlated query, multiple correlated variables referring to different outer var
explain select * from part where p_name NOT IN (select p_name from part p where p.p_size = part.p_size AND part.p_partkey= p.p_partkey );
select * from part where p_name NOT IN (select p_name from part p where p.p_size = part.p_size AND part.p_partkey= p.p_partkey );

-- correlated var refers to outer table alias
explain select p_name from (select p_name, p_type, p_brand as brand from part) fpart where fpart.p_type NOT IN (select p_type+2 from part where part.p_brand = fpart.brand);
select p_name from (select p_name, p_type, p_brand as brand from part) fpart where fpart.p_type NOT IN (select p_type+2 from part where part.p_brand = fpart.brand);
 
-- correlated var refers to outer table alias which is an expression 
explain select p_name from (select p_name, p_type, p_size+1 as size from part) fpart where fpart.p_type NOT IN (select p_type from part where (part.p_size+1) = fpart.size);
select p_name from (select p_name, p_type, p_size+1 as size from part) fpart where fpart.p_type NOT IN (select p_type from part where (part.p_size+1) = fpart.size+1);

-- where plus having
explain select key, count(*) from src where value NOT IN (select key from src) group by key having count(*) in (select count(*) from src s1 where s1.key = '90' group by s1.key );
select key, count(*) from src where value NOT IN (select key from src) group by key having count(*) in (select count(*) from src s1 where s1.key = '90' group by s1.key );

-- where with having, correlated
explain select key, count(*) from src where value NOT IN (select concat('v', value) from src sc where sc.key = src.key ) group by key having count(*) in (select count(*) from src s1 where s1.key = '90' group by s1.key );
select key, count(*) from src where value NOT IN (select concat('v', value) from src sc where sc.key = src.key ) group by key having count(*) in (select count(*) from src s1 where s1.key = '90' group by s1.key );

-- subquery with order by
explain select * from part  where (p_size-1) NOT IN (select min(p_size) from part group by p_type) order by p_brand;
select * from part  where (p_size-1) NOT IN (select min(p_size) from part group by p_type) order by p_brand;

--order by with limit
explain select * from part  where (p_size-1) NOT IN (select min(p_size) from part group by p_type) order by p_brand, p_partkey limit 4;
select * from part  where (p_size-1) NOT IN (select min(p_size) from part group by p_type) order by p_brand, p_partkey limit 4;

-- union, uncorr
explain select * from src where key NOT IN (select p_name from part UNION ALL select p_brand from part);
select * from src where key NOT IN (select p_name from part UNION ALL select p_brand from part);

explain select count(*) as c from part as e where p_size + 100 not in ( select p_type from part where p_brand = e.p_brand);
select count(*) as c from part as e where p_size + 100 not in ( select p_type from part where p_brand = e.p_brand);

--nullability tests
CREATE TABLE t1 (c1 INT, c2 CHAR(100));
INSERT INTO t1 VALUES (null,null), (1,''), (2,'abcde'), (100,'abcdefghij');

CREATE TABLE t2 (c1 INT);
INSERT INTO t2 VALUES (null), (2), (100);

-- uncorr
explain SELECT c1 FROM t1 WHERE c1 NOT IN (SELECT c1 FROM t2);
SELECT c1 FROM t1 WHERE c1 NOT IN (SELECT c1 FROM t2);

-- corr
explain SELECT c1 FROM t1 WHERE c1 NOT IN (SELECT c1 FROM t2 where t1.c2=t2.c1);
SELECT c1 FROM t1 WHERE c1 NOT IN (SELECT c1 FROM t2 where t1.c1=t2.c1);

DROP TABLE t1;
DROP TABLE t2;

-- corr, nullability, should not produce any result
create table t1(a int, b int);
insert into t1 values(1,0), (1,0),(1,0);

create table t2(a int, b int);
insert into t2 values(2,1), (3,1), (NULL,1);

explain select t1.a from t1 where t1.b NOT IN (select t2.a from t2 where t2.b=t1.a);
select t1.a from t1 where t1.b NOT IN (select t2.a from t2 where t2.b=t1.a);
drop table t1;
drop table t2;


-- coor, nullability, should produce result
create table t7(i int, j int);
insert into t7 values(null, 5), (4, 15);

create table fixOb(i int, j int);
insert into fixOb values(-1, 5), (-1, 15);

explain select * from fixOb where j NOT IN (select i from t7 where t7.j=fixOb.j);
select * from fixOb where j NOT IN (select i from t7 where t7.j=fixOb.j);

drop table t7;
drop table fixOb;

create table t(i int, j int);
insert into t values(1,2), (4,5), (7, NULL);


-- case with empty inner result (t1.j=t.j=NULL) and null subquery key(t.j = NULL)
explain select t.i from t where t.j NOT IN (select t1.i from t t1 where t1.j=t.j);
select t.i from t where t.j NOT IN (select t1.i from t t1 where t1.j=t.j);

-- case with empty inner result (t1.j=t.j=NULL) and non-null subquery key(t.i is never null)
explain select t.i from t where t.i NOT IN (select t1.i from t t1 where t1.j=t.j);
select t.i from t where t.i NOT IN (select t1.i from t t1 where t1.j=t.j);

-- case with non-empty inner result and null subquery key(t.j is null)
explain select t.i from t where t.j NOT IN (select t1.i from t t1 );
select t.i from t where t.j NOT IN (select t1.i from t t1 );

-- case with non-empty inner result and non-null subquery key(t.i is never null)
explain select t.i from t where t.i NOT IN (select t1.i from t t1 );
select t.i from t where t.i NOT IN (select t1.i from t t1 );

drop table t1;

-- corr predicate is not equi
explain select *
from src b
where b.key not in
        (select a.key
         from src a
         where b.value > a.value and a.key > '9'
        )
;
select *
from src b
where b.key not in
        (select a.key
         from src a
         where b.value > a.value and a.key > '9'
        );

