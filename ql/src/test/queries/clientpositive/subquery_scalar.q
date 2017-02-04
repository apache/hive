set hive.mapred.mode=nonstrict;
set hive.explain.user=false;

create table tnull(i int, c char(2));
insert into tnull values(NULL, NULL), (NULL, NULL);

create table tempty(c char(2));

CREATE TABLE part_null(
    p_partkey INT,
    p_name STRING,
    p_mfgr STRING,
    p_brand STRING,
    p_type STRING,
    p_size INT,
    p_container STRING,
    p_retailprice DOUBLE,
    p_comment STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
;

LOAD DATA LOCAL INPATH '../../data/files/part_tiny_nulls.txt' overwrite into table part_null;

insert into part_null values(78487,NULL,'Manufacturer#6','Brand#52','LARGE BRUSHED BRASS', 23, 'MED BAG',1464.48,'hely blith');


-- non corr, simple less than  
explain select * from part where p_size > (select avg(p_size) from part_null);
select * from part where p_size > (select avg(p_size) from part_null);

-- non corr, empty
select * from part where p_size > (select * from tempty);
explain select * from part where p_size > (select * from tempty);

-- non corr, null comparison
explain select * from part where p_name = (select p_name from part_null where p_name is null);
select * from part where p_name = (select p_name from part_null where p_name is null);

-- non corr, is null 
explain select * from part where (select i from tnull limit 1) is null;
select * from part where (select i from tnull limit 1) is null;

-- non corr, is not null
explain select * from part where (select max(p_name) from part_null) is not null;
select * from part where (select max(p_name) from part_null) is not null;

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


-- corr, equi-join predicate
explain select * from part where p_size > (select avg(p_size) from part_null where part_null.p_type = part.p_type);
select * from part where p_size > (select avg(p_size) from part_null where part_null.p_type = part.p_type);

-- mix of corr and uncorr
explain select * from part where p_size BETWEEN (select min(p_size) from part_null where part_null.p_type = part.p_type) AND (select max(p_size) from part_null);
select * from part where p_size BETWEEN (select min(p_size) from part_null where part_null.p_type = part.p_type) AND (select max(p_size) from part_null);

-- mix of corr and uncorr
explain select * from part where p_size >= (select min(p_size) from part_null where part_null.p_type = part.p_type) AND p_retailprice <= (select max(p_retailprice) from part_null);
select * from part where p_size >= (select min(p_size) from part_null where part_null.p_type = part.p_type) AND p_retailprice <= (select max(p_retailprice) from part_null);

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
explain select * from part_null where p_name NOT LIKE (select min(p_name) from part_null) AND p_brand NOT IN (select p_name from part);
select * from part_null where p_name NOT LIKE (select min(p_name) from part_null) AND p_brand NOT IN (select p_name from part);

-- mix of NOT IN and corr scalar
explain select * from part_null where p_brand NOT IN (select p_name from part) AND p_name NOT LIKE (select min(p_name) from part_null pp where part_null.p_type = pp.p_type);
select * from part_null where p_brand NOT IN (select p_name from part) AND p_name NOT LIKE (select min(p_name) from part_null pp where part_null.p_type = pp.p_type);

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
explain select * from part_null where p_name IN (select p_name from part where part.p_type = part_null.p_type AND p_brand NOT LIKE (select min(p_brand) from part pp where part.p_type = pp.p_type));
select * from part_null where p_name IN (select p_name from part where part.p_type = part_null.p_type AND p_brand NOT LIKE (select min(p_brand) from part pp where part.p_type = pp.p_type));

drop table tnull;
drop table part_null;
drop table tempty;


create table EMPS(EMPNO int,NAME string,DEPTNO int,GENDER string,CITY string,EMPID int,AGE int,SLACKER boolean,MANAGER boolean,JOINEDAT date);

insert into EMPS values (100,'Fred',10,NULL,NULL,30,25,true,false,'1996-08-03');
insert into EMPS values (110,'Eric',20,'M','San Francisco',3,80,NULL,false,'2001-01-01') ;
insert into EMPS values (110,'John',40,'M','Vancouver',2,NULL,false,true,'2002-05-03');
insert into EMPS values (120,'Wilma',20,'F',NULL,1,5,NULL,true,'2005-09-07');
insert into EMPS values (130,'Alice',40,'F','Vancouver',2,NULL,false,true,'2007-01-01');

create table DEPTS(deptno int, name string);
insert into DEPTS values( 10,'Sales');
insert into DEPTS values( 20,'Marketing');
insert into DEPTS values( 30,'Accounts');

-- corr, scalar, with count aggregate
explain select * from emps where deptno <> (select count(deptno) from depts where depts.name = emps.name);
select * from emps where deptno <> (select count(deptno) from depts where depts.name = emps.name);

explain select * from emps where name > (select min(name) from depts where depts.deptno=emps.deptno);
select * from emps where name > (select min(name) from depts where depts.deptno=emps.deptno);

-- corr, scalar multiple subq with count aggregate
explain select * from emps where deptno <> (select count(deptno) from depts where depts.name = emps.name) and empno > (select count(name) from depts where depts.deptno = emps.deptno);
select * from emps where deptno <> (select count(deptno) from depts where depts.name = emps.name) and empno > (select count(name) from depts where depts.deptno = emps.deptno);

-- mix of corr, uncorr with aggregate
explain select * from emps where deptno <> (select sum(deptno) from depts where depts.name = emps.name) and empno > (select count(name) from depts);
select * from emps where deptno <> (select count(deptno) from depts where depts.name = emps.name) and empno > (select count(name) from depts);

drop table DEPTS;
drop table EMPS;

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

