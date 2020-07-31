--! qt:dataset:src
--! qt:dataset:part
--! qt:dataset:lineitem
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
-- SORT_QUERY_RESULTS

create table tnull_n0(i int, c char(2));
insert into tnull_n0 values(NULL, NULL), (NULL, NULL);

create table tempty_n0(c char(2));

CREATE TABLE part_null_n0(
    p_partkey INT,
    p_name STRING,
    p_mfgr STRING,
    p_brand STRING,
    p_type STRING,
    p_size INT,
    p_container STRING,
    p_retailprice DOUBLE,
    p_comment_n11 STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
;

LOAD DATA LOCAL INPATH '../../data/files/part_tiny_nulls.txt' overwrite into table part_null_n0;

insert into part_null_n0 values(78487,NULL,'Manufacturer#6','Brand#52','LARGE BRUSHED BRASS', 23, 'MED BAG',1464.48,'hely blith');


-- non corr, simple less than  
explain select * from part where p_size > (select avg(p_size) from part_null_n0);
select * from part where p_size > (select avg(p_size) from part_null_n0);

-- non corr, empty
select * from part where p_size > (select * from tempty_n0);
explain select * from part where p_size > (select * from tempty_n0);

-- non corr, null comparison
explain select * from part where p_name = (select p_name from part_null_n0 where p_name is null);
select * from part where p_name = (select p_name from part_null_n0 where p_name is null);

-- non corr, is null 
explain select * from part where (select i from tnull_n0 limit 1) is null;
select * from part where (select i from tnull_n0 limit 1) is null;

-- non corr, is not null
explain select * from part where (select max(p_name) from part_null_n0) is not null;
select * from part where (select max(p_name) from part_null_n0) is not null;

-- non corr, between
explain select * from part where p_size between (select min(p_size) from part) and (select avg(p_size) from part);
select * from part where p_size between (select min(p_size) from part) and (select avg(p_size) from part);

-- non corr, windowing
explain select p_mfgr, p_name, p_size from part 
where part.p_size > 
  (select first_value(p_size) over(partition by p_mfgr order by p_size) as fv from part order by fv limit 1);
select p_mfgr, p_name, p_size from part 
where part.p_size > 
  (select first_value(p_size) over(partition by p_mfgr order by p_size) as fv from part order by fv limit 1);


-- lhs contain complex expressions
explain select * from part where (p_partkey*p_size) <> (select min(p_partkey) from part);
select * from part where (p_partkey*p_size) <> (select min(p_partkey) from part);

-- corr, lhs contain complex expressions
explain select count(*) as c from part as e where p_size + 100 < (select max(p_partkey) from part where p_name = e.p_name);
select count(*) as c from part as e where p_size + 100 < (select max(p_partkey) from part where p_name = e.p_name);

-- corr, lhs contain constant expressions (HIVE-16689)
explain select count(*) as c from part as e where 100 < (select max(p_partkey) from part where p_name = e.p_name);
select count(*) as c from part as e where 100 < (select max(p_partkey) from part where p_name = e.p_name);


-- corr, equi-join predicate
explain select * from part where p_size > (select avg(p_size) from part_null_n0 where part_null_n0.p_type = part.p_type);
select * from part where p_size > (select avg(p_size) from part_null_n0 where part_null_n0.p_type = part.p_type);

-- mix of corr and uncorr
explain select * from part where p_size BETWEEN (select min(p_size) from part_null_n0 where part_null_n0.p_type = part.p_type) AND (select max(p_size) from part_null_n0);
select * from part where p_size BETWEEN (select min(p_size) from part_null_n0 where part_null_n0.p_type = part.p_type) AND (select max(p_size) from part_null_n0);

-- mix of corr and uncorr
explain select * from part where p_size >= (select min(p_size) from part_null_n0 where part_null_n0.p_type = part.p_type) AND p_retailprice <= (select max(p_retailprice) from part_null_n0);
select * from part where p_size >= (select min(p_size) from part_null_n0 where part_null_n0.p_type = part.p_type) AND p_retailprice <= (select max(p_retailprice) from part_null_n0);

-- mix of scalar and IN corr 
explain select * from part where p_brand <> (select min(p_brand) from part ) AND p_size IN (select (p_size) from part p where p.p_type = part.p_type ) AND p_size <> 340;
select * from part where p_brand <> (select min(p_brand) from part ) AND p_size IN (select (p_size) from part p where p.p_type = part.p_type ) AND p_size <> 340;

-- multiple corr var with scalar query
explain select * from part where p_size <> (select count(p_name) from part p where p.p_size = part.p_size AND part.p_partkey= p.p_partkey );
select * from part where p_size <> (select count(p_name) from part p where p.p_size = part.p_size AND part.p_partkey= p.p_partkey );

-- where + having
explain select key, count(*) from src where value <> (select max(value) from src) group by key having count(*) > (select count(*) from src s1 where s1.key = '90' group by s1.key );
select key, count(*) from src where value <> (select max(value) from src) group by key having count(*) > (select count(*) from src s1 where s1.key = '90' group by s1.key );

explain select sum(p_retailprice) from part group by p_type having sum(p_retailprice) > (select max(pp.p_retailprice) from part pp);
select sum(p_retailprice) from part group by p_type having sum(p_retailprice) > (select max(pp.p_retailprice) from part pp);

-- scalar subquery with INTERSECT
explain select * from part where p_size > (select count(p_name) from part INTERSECT select count(p_brand) from part);
select * from part where p_size > (select count(p_name) from part INTERSECT select count(p_brand) from part);

-- join in subquery
explain select p_partkey from part where p_name like (select max(p.p_name) from part p left outer join part pp on p.p_type = pp.p_type where pp.p_size = part.p_size);
select p_partkey from part where p_name like (select max(p.p_name) from part p left outer join part pp on p.p_type = pp.p_type where pp.p_size = part.p_size);

-- mix of NOT IN and scalar
explain select * from part_null_n0 where p_name NOT LIKE (select min(p_name) from part_null_n0) AND p_brand NOT IN (select p_name from part);
select * from part_null_n0 where p_name NOT LIKE (select min(p_name) from part_null_n0) AND p_brand NOT IN (select p_name from part);

-- mix of NOT IN and corr scalar
explain select * from part_null_n0 where p_brand NOT IN (select p_name from part) AND p_name NOT LIKE (select min(p_name) from part_null_n0 pp where part_null_n0.p_type = pp.p_type);
select * from part_null_n0 where p_brand NOT IN (select p_name from part) AND p_name NOT LIKE (select min(p_name) from part_null_n0 pp where part_null_n0.p_type = pp.p_type);

-- non corr, with join in parent query
explain select p.p_partkey, li.l_suppkey 
from (select distinct l_partkey as p_partkey from lineitem) p join lineitem li on p.p_partkey = li.l_partkey 
where li.l_linenumber = 1 and
li.l_orderkey <> (select min(l_orderkey) from lineitem where l_shipmode = 'AIR' and l_linenumber = li.l_linenumber)
;
select p.p_partkey, li.l_suppkey 
from (select distinct l_partkey as p_partkey from lineitem) p join lineitem li on p.p_partkey = li.l_partkey 
where li.l_linenumber = 1 and
li.l_orderkey <> (select min(l_orderkey) from lineitem where l_shipmode = 'AIR' and l_linenumber = li.l_linenumber)
;

-- corr, with join in outer query
explain select p.p_partkey, li.l_suppkey 
from (select distinct l_partkey as p_partkey from lineitem) p join lineitem li on p.p_partkey = li.l_partkey 
where li.l_linenumber = 1 and
 li.l_orderkey <> (select min(l_orderkey) from lineitem where l_shipmode = 'AIR' and l_linenumber = li.l_linenumber);
select p.p_partkey, li.l_suppkey 
from (select distinct l_partkey as p_partkey from lineitem) p join lineitem li on p.p_partkey = li.l_partkey 
where li.l_linenumber = 1 and
li.l_orderkey <> (select min(l_orderkey) from lineitem where l_shipmode = 'AIR' and l_linenumber = li.l_linenumber);

-- corr, aggregate in outer
explain select sum(l_extendedprice) from lineitem, part where p_partkey = l_partkey and l_quantity > (select avg(l_quantity) from lineitem where l_partkey = p_partkey);
select sum(l_extendedprice) from lineitem, part where p_partkey = l_partkey and l_quantity > (select avg(l_quantity) from lineitem where l_partkey = p_partkey);

-- nested with scalar
explain select * from part_null_n0 where p_name IN (select p_name from part where part.p_type = part_null_n0.p_type AND p_brand NOT LIKE (select min(p_brand) from part pp where part.p_type = pp.p_type));
select * from part_null_n0 where p_name IN (select p_name from part where part.p_type = part_null_n0.p_type AND p_brand NOT LIKE (select min(p_brand) from part pp where part.p_type = pp.p_type));

-- non corr, is null , is not converted to anti join.
explain select * from part where (select i from tnull_n0 limit 1) is null;
select * from part where (select i from tnull_n0 limit 1) is null;

drop table tnull_n0;
drop table part_null_n0;
drop table tempty_n0;


create table EMPS_n4(EMPNO int,NAME string,DEPTNO int,GENDER string,CITY string,EMPID int,AGE int,SLACKER boolean,MANAGER boolean,JOINEDAT date);

insert into EMPS_n4 values (100,'Fred',10,NULL,NULL,30,25,true,false,'1996-08-03');
insert into EMPS_n4 values (110,'Eric',20,'M','San Francisco',3,80,NULL,false,'2001-01-01') ;
insert into EMPS_n4 values (110,'John',40,'M','Vancouver',2,NULL,false,true,'2002-05-03');
insert into EMPS_n4 values (120,'Wilma',20,'F',NULL,1,5,NULL,true,'2005-09-07');
insert into EMPS_n4 values (130,'Alice',40,'F','Vancouver',2,NULL,false,true,'2007-01-01');

create table DEPTS_n3(deptno int, name string);
insert into DEPTS_n3 values( 10,'Sales');
insert into DEPTS_n3 values( 20,'Marketing');
insert into DEPTS_n3 values( 30,'Accounts');

-- corr, scalar, with count aggregate
explain select * from emps_n4 where deptno <> (select count(deptno) from depts_n3 where depts_n3.name = emps_n4.name);
select * from emps_n4 where deptno <> (select count(deptno) from depts_n3 where depts_n3.name = emps_n4.name);

explain select * from emps_n4 where name > (select min(name) from depts_n3 where depts_n3.deptno=emps_n4.deptno);
select * from emps_n4 where name > (select min(name) from depts_n3 where depts_n3.deptno=emps_n4.deptno);

-- corr, scalar multiple subq with count aggregate
explain select * from emps_n4 where deptno <> (select count(deptno) from depts_n3 where depts_n3.name = emps_n4.name) and empno > (select count(name) from depts_n3 where depts_n3.deptno = emps_n4.deptno);
select * from emps_n4 where deptno <> (select count(deptno) from depts_n3 where depts_n3.name = emps_n4.name) and empno > (select count(name) from depts_n3 where depts_n3.deptno = emps_n4.deptno);

-- mix of corr, uncorr with aggregate
explain select * from emps_n4 where deptno <> (select sum(deptno) from depts_n3 where depts_n3.name = emps_n4.name) and empno > (select count(name) from depts_n3);
select * from emps_n4 where deptno <> (select count(deptno) from depts_n3 where depts_n3.name = emps_n4.name) and empno > (select count(name) from depts_n3);

drop table DEPTS_n3;
drop table EMPS_n4;

-- having
explain
 select key, count(*)
from src
group by key
having count(*) > (select count(*) from src s1 where s1.key > '9' )
;

select key, count(*)
from src
group by key
having count(*) > (select count(*) from src s1 where s1.key = '90')
;

explain
select key, value, count(*)
from src b
where b.key in (select key from src where src.key > '8')
group by key, value
having count(*) > (select count(*) from src s1 where s1.key > '9' )
;
select key, value, count(*)
from src b
where b.key in (select key from src where src.key > '8')
group by key, value
having count(*) > (select count(*) from src s1 where s1.key > '9' )
;

-- since subquery has implicit group by this should have sq_count_check (HIVE-16793)
explain  select * from part where p_size > (select max(p_size) from part group by p_type);
-- same as above, for correlated columns
explain  select * from part where p_size > (select max(p_size) from part p where p.p_type = part.p_type group by p_type);

-- corr scalar subquery with aggregate, having non-equi corr predicate
explain select * from part where p_size <>
    (select count(p_size) from part pp where part.p_type <> pp.p_type);
select * from part where p_size <>
    (select count(p_size) from part pp where part.p_type <> pp.p_type);

create table t_n11(i int, j int);
insert into t_n11 values(3,1), (1,1);

-- for t_n11.i=1 inner query will result empty result, making count(*) = 0
--   therefore where predicate will be true
explain select * from t_n11 where 0 = (select count(*) from t_n11 tt_n11 where tt_n11.j <> t_n11.i);
select * from t_n11 where 0 = (select count(*) from t_n11 tt_n11 where tt_n11.j <> t_n11.i);

-- same as above but with avg aggregate, avg(tt_n11.i) will be null therefore
-- empty result set
explain select * from t_n11 where 0 = (select avg(tt_n11.i) from t_n11 tt_n11 where tt_n11.j <> t_n11.i);
select * from t_n11 where 0 = (select avg(tt_n11.i) from t_n11 tt_n11 where tt_n11.j <> t_n11.i);

create table tempty_n0(i int, j int);

-- following query has subquery on empty making count(*) to zero and where predicate
-- to true for all rows in outer query
explain select * from t_n11 where 0 = (select count(*) from tempty_n0 tt_n11 where t_n11.i=tt_n11.i);
select * from t_n11 where 0 = (select count(*) from tempty_n0 tt_n11 where t_n11.i=tt_n11.i);

-- same as above but with min aggregate, since min on empty will return null
-- making where predicate false for all
explain select * from t_n11 where 0 = (select min(tt_n11.j) from tempty_n0 tt_n11 where t_n11.i=tt_n11.i);
select * from t_n11 where 0 = (select min(tt_n11.j) from tempty_n0 tt_n11 where t_n11.i=tt_n11.i);

drop table t_n11;
drop table tempty_n0;

-- following queries shouldn't have a join with sq_count_check
explain select key, count(*) from src group by key having count(*) >
    (select count(*) from src s1 group by 4);

explain select key, count(*) from src group by key having count(*) >
    (select count(*) from src s1 where s1.key = '90' group by s1.key );


CREATE TABLE `store_sales`(
  `ss_sold_date_sk` int,
  `ss_quantity` int,
  `ss_list_price` decimal(7,2));

CREATE TABLE `date_dim`(
  `d_date_sk` int,
  `d_year` int);

explain cbo with avg_sales as
 (select avg(quantity*list_price) average_sales
  from (select ss_quantity quantity
             ,ss_list_price list_price
       from store_sales
           ,date_dim
       where ss_sold_date_sk = d_date_sk
         and d_year between 1999 and 2001 ) x)
select * from store_sales where ss_list_price > (select average_sales from avg_sales);

-- this one should have sq_count_check branch because it contains windowing function
explain cbo with avg_sales as
 (select avg(quantity*list_price) over( partition by list_price) average_sales
  from (select ss_quantity quantity
             ,ss_list_price list_price
       from store_sales
           ,date_dim
       where ss_sold_date_sk = d_date_sk
         and d_year between 1999 and 2001 ) x)
select * from store_sales where ss_list_price > (select average_sales from avg_sales);
DROP TABLE store_sales;
DROP TABLE date_dim;

