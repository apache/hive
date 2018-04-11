set hive.mapred.mode=nonstrict;
set hive.explain.user=false;

-- SORT_QUERY_RESULTS

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

-- multiple subquery

-- Both IN are always true so should return all rows
explain select * from part_null where p_size IN (select p_size from part_null) AND p_brand IN (select p_brand from part_null);
select * from part_null where p_size IN (select p_size from part_null) AND p_brand IN (select p_brand from part_null);

-- NOT IN has null value so should return 0 rows
explain select * from part_null where p_name IN (select p_name from part_null) AND p_brand NOT IN (select p_name from part_null);
select * from part_null where p_name IN (select p_name from part_null) AND p_brand NOT IN (select p_name from part_null);

-- NOT IN is always true and IN is false for where p_name is NULL, hence should return all but one row
explain select * from part_null where p_name IN (select p_name from part_null) AND p_brand NOT IN (select p_type from part_null);
select * from part_null where p_name IN (select p_name from part_null) AND p_brand NOT IN (select p_type from part_null);

-- NOT IN has one NULL value so this whole query should not return any row
explain select * from part_null where p_brand IN (select p_brand from part_null) AND p_brand NOT IN (select p_name from part_null);
select * from part_null where p_brand IN (select p_brand from part_null) AND p_brand NOT IN (select p_name from part_null);

-- NOT IN is always true irrespective of p_name being null/non-null since inner query is empty
-- second query is always true so this should return all rows
explain select * from part_null where p_name NOT IN (select c from tempty) AND p_brand IN (select p_brand from part_null);
select * from part_null where p_name NOT IN (select c from tempty) AND p_brand IN (select p_brand from part_null);

-- IN, EXISTS
explain select * from part_null where p_name IN (select p_name from part_null) AND EXISTS (select c from tnull);
select * from part_null where p_name IN (select p_name from part_null) AND EXISTS (select c from tnull);

explain select * from part_null where p_size IN (select p_size from part_null) AND EXISTS (select c from tempty);
select * from part_null where p_size IN (select p_size from part_null) AND EXISTS (select c from tempty);

explain select * from part_null where p_name IN (select p_name from part_null) AND NOT EXISTS (select c from tempty);
select * from part_null where p_name IN (select p_name from part_null) AND NOT EXISTS (select c from tempty);

-- corr, mix of IN/NOT IN
explain select * from part_null where p_name IN ( select p_name from part where part.p_type = part_null.p_type)
        AND p_brand NOT IN (select p_container from part where part.p_type = part_null.p_type
                                AND p_brand IN (select p_brand from part pp where part.p_type = pp.p_type));
select * from part_null where p_name IN ( select p_name from part where part.p_type = part_null.p_type)
        AND p_brand NOT IN (select p_container from part where part.p_type = part_null.p_type
                                AND p_brand IN (select p_brand from part pp where part.p_type = pp.p_type));

-- mix of corr and uncorr
explain select * from part_null where p_name IN ( select p_name from part) AND p_brand IN (select p_brand from part where part.p_type = part_null.p_type);
select * from part_null where p_name IN ( select p_name from part) AND p_brand IN (select p_brand from part where part.p_type = part_null.p_type);

-- one query has multiple corr
explain select * from part_null where p_name IN ( select p_name from part where part.p_type = part_null.p_type AND part.p_container=part_null.p_container) AND p_brand NOT IN (select p_container from part where part.p_type = part_null.p_type AND p_brand IN (select p_brand from part pp where part.p_type = pp.p_type));
select * from part_null where p_name IN ( select p_name from part where part.p_type = part_null.p_type AND part.p_container=part_null.p_container) AND p_brand NOT IN (select p_container from part where part.p_type = part_null.p_type AND p_brand IN (select p_brand from part pp where part.p_type = pp.p_type));

--diff corr var (all reffering to diff outer var)
explain select * from part_null where p_name IN (select p_name from part where part.p_type = part_null.p_type) AND p_brand NOT IN (select p_type from part where part.p_size = part_null.p_size);
select * from part_null where p_name IN (select p_name from part where part.p_type = part_null.p_type) AND p_brand NOT IN (select p_type from part where part.p_size = part_null.p_size);

-- NESTED QUERIES
-- both queries are correlated
explain select * from part_null where p_name IN (select p_name from part where part.p_type = part_null.p_type AND p_brand IN (select p_brand from part pp where part.p_type = pp.p_type));
select * from part_null where p_name IN (select p_name from part where part.p_type = part_null.p_type AND p_brand IN (select p_brand from part pp where part.p_type = pp.p_type));

-- in, not in corr
explain select p.p_partkey, li.l_suppkey
from (select distinct l_partkey as p_partkey from lineitem) p join lineitem li on p.p_partkey = li.l_partkey
where li.l_linenumber = 1 and
 li.l_orderkey in (select l_orderkey from lineitem where l_shipmode = 'AIR' and l_linenumber = li.l_linenumber AND l_quantity NOT IN (select avg(l_quantity) from lineitem));
select p.p_partkey, li.l_suppkey
from (select distinct l_partkey as p_partkey from lineitem) p join lineitem li on p.p_partkey = li.l_partkey
where li.l_linenumber = 1 and
 li.l_orderkey in (select l_orderkey from lineitem where l_shipmode = 'AIR' and l_linenumber = li.l_linenumber AND l_quantity NOT IN (select avg(l_quantity) from lineitem));

explain
select key, value, count(*)
from src b
where b.key in (select key from src where src.value = b.value)
group by key, value
having count(*) in (select count(*) from src s1 where s1.key > '9' and exists (select * from src s2 where s1.value = s2.value) group by s1.key )
 ;
select key, value, count(*)
from src b
where b.key in (select key from src where src.value = b.value)
group by key, value
having count(*) in (select count(*) from src s1 where s1.key > '9' and exists (select * from src s2 where s1.value = s2.value) group by s1.key ) ;

-- subquery pred only refer to parent query column
explain select * from part where p_name IN (select p_name from part p where part.p_type <> '1');
select * from part where p_name IN (select p_name from part p where part.p_type <> '1');

-- OR subqueries
insert into tnull values(1, 'c');
explain select * from part where p_partkey = 3 OR p_size NOT IN (select i from tnull);
select * from part where p_partkey = 3 OR p_size NOT IN (select i from tnull);

explain select count(*)  from src
    where src.key in (select key from src s1 where s1.key > '9')
        or src.value is not null
        or exists(select key from src);

select count(*)  from src
    where src.key in (select key from src s1 where s1.key > '9')
        or src.value is not null
        or exists(select key from src);

-- EXISTS and NOT EXISTS with non-equi predicate
explain select * from part ws1 where
    exists (select * from part ws2 where ws1.p_type= ws2.p_type
                            and ws1.p_retailprice <> ws2.p_retailprice)
    and not exists(select * from part_null wr1 where ws1.p_type = wr1.p_name);
select * from part ws1 where
    exists (select * from part ws2 where ws1.p_type= ws2.p_type
                            and ws1.p_retailprice <> ws2.p_retailprice)
    and not exists(select * from part_null wr1 where ws1.p_type = wr1.p_name);


drop table tnull;
drop table tempty;
drop table part_null;

