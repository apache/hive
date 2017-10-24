-- following tests test queries in SELECT
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;

-- IN, non corr
explain SELECT p_size, p_size IN (
        SELECT MAX(p_size) FROM part)
FROM part;

SELECT p_size, p_size IN (
        SELECT MAX(p_size) FROM part)
FROM part ;

-- IN, corr
EXPLAIN SELECT p_size, p_size IN (
        SELECT MAX(p_size) FROM part p where p.p_type = part.p_type)
FROM part;

SELECT p_size, p_size IN (
        SELECT MAX(p_size) FROM part p where p.p_type = part.p_type)
FROM part;

-- NOT IN, non corr
explain SELECT p_size, p_size NOT IN (
        SELECT MAX(p_size) FROM part)
FROM part;

SELECT p_size, p_size NOT IN (
        SELECT MAX(p_size) FROM part)
FROM part ;

-- NOT IN, corr
EXPLAIN SELECT p_size, p_size NOT IN (
        SELECT MAX(p_size) FROM part p where p.p_type = part.p_type)
FROM part;

SELECT p_size, p_size NOT IN (
        SELECT MAX(p_size) FROM part p where p.p_type = part.p_type)
FROM part;

-- EXISTS, non corr
explain SELECT p_size, EXISTS(SELECT p_size FROM part)
FROM part;

SELECT p_size, EXISTS(SELECT p_size FROM part)
FROM part;

-- EXISTS, corr
explain SELECT p_size, EXISTS(SELECT p_size FROM part pp where pp.p_type = part.p_type)
FROM part;

SELECT p_size, EXISTS(SELECT p_size FROM part pp where pp.p_type = part.p_type)
FROM part;

-- NOT EXISTS, non corr
explain SELECT p_size, NOT EXISTS(SELECT p_size FROM part)
FROM part;

SELECT p_size, NOT EXISTS(SELECT p_size FROM part)
FROM part;

-- NOT EXISTS, corr
explain SELECT p_size, NOT EXISTS(SELECT p_size FROM part pp where pp.p_type = part.p_type)
FROM part;

SELECT p_size, NOT EXISTS(SELECT p_size FROM part pp where pp.p_type = part.p_type)
FROM part;

-- scalar with COUNT, since where is always false count should return 0
explain select p_size, (select count(p_name) from part p where p.p_type = part.p_name) from part;
select p_size, (select count(p_name) from part p where p.p_type = part.p_name) from part;

-- scalar with MAX, since where is always false max should return NULL
explain select p_size, (select max(p_name) from part p where p.p_type = part.p_name) from part;
select p_size, (select max(p_name) from part p where p.p_type = part.p_name) from part;

-- SCALAR, non corr
explain SELECT p_size, (SELECT max(p_size) FROM part)
    FROM part;

SELECT p_size, (SELECT max(p_size) FROM part)
    FROM part;

-- IN, corr with scalar
explain
select *
from src b
where b.key in
        (select (select max(key) from src)
         from src a
         where b.value = a.value and a.key > '9'
        );
select *
from src b
where b.key in
        (select (select max(key) from src)
         from src a
         where b.value = a.value and a.key > '9'
        );

-- corr within corr..correcionnn..
explain
select *
from src b
where b.key in
        (select (select max(key) from src sc where sc.value = a.value)
         from src a
         where b.value = a.value and a.key > '9'
        );

select *
from src b
where b.key in
        (select (select max(key) from src sc where sc.value = a.value)
         from src a
         where b.value = a.value and a.key > '9' );

CREATE table tnull(i int);
insert into tnull values(null);

-- IN query returns unknown/NULL instead of true/false
explain select p_size, p_size IN (select i from tnull) from part;
select p_size, p_size IN (select i from tnull) from part;

CREATE TABLE tempty(i int);

explain select p_size, (select count(*) from tempty) from part;
select p_size, (select count(*) from tempty) from part;

explain select p_size, (select max(i) from tempty) from part;
select p_size, (select max(i) from tempty) from part;

DROP table tempty;
DROP table tnull;

-- following tests test subquery in all kind of expressions (except UDAF, UDA and UDTF)



-- different data types
-- string with string
-- null with int
-- boolean (IN, EXISTS) with AND, OR

-- scalar, corr
explain SELECT p_size, 1+(SELECT max(p_size) FROM part p
    WHERE p.p_type = part.p_type) from part;
SELECT p_size, 1+(SELECT max(p_size) FROM part p
    WHERE p.p_type = part.p_type) from part;

-- IS NULL
explain SELECT p_size, (SELECT count(p_size) FROM part p
    WHERE p.p_type = part.p_type) IS NULL from part;
SELECT p_size, (SELECT count(p_size) FROM part p
    WHERE p.p_type = part.p_type) IS NULL from part;

-- scalar, non-corr, non agg
explain select p_type, (select p_size from part order by p_size limit 1) = 1 from part order by p_type;
select p_type, (select p_size from part order by p_size limit 1) = 1 from part order by p_type;

-- in corr, multiple
EXPLAIN SELECT p_size, p_size IN (
        SELECT MAX(p_size) FROM part p where p.p_type = part.p_type) AND
        p_name IN (SELECT min(p_name) from part)
FROM part;
SELECT p_size, p_size IN (
        SELECT MAX(p_size) FROM part p where p.p_type = part.p_type) AND
        p_name IN (SELECT min(p_name) from part)
FROM part;

-- exists, corr
explain SELECT p_size, NOT EXISTS(SELECT p_size FROM part pp where pp.p_type = part.p_type)
FROM part;
SELECT p_size, NOT EXISTS(SELECT p_size FROM part pp where pp.p_type = part.p_type)
FROM part;

-- scalar subquery within IN subquery
explain select p_size, (p_size IN
    (select (select max(p_size) from part) as sb from part order by sb limit 1)) = true
   from part;
select p_size, (p_size IN
    (select (select max(p_size) from part) as sb from part order by sb limit 1)) = true
   from part;

explain select case when (select count(*)
                  from part
                  where p_size between 1 and 20) > 409437
            then (select avg(p_partkey)
                  from part
                  where p_partkey between 1 and 20)
            else (select max(p_size)
                  from part
                  where p_partkey between 10000 and 20000) end sq
from part;

select case when (select count(*)
                  from part
                  where p_size between 1 and 20) > 409437
            then (select avg(p_partkey)
                  from part
                  where p_partkey between 1 and 20)
            else (select max(p_size)
                  from part
                  where p_partkey between 10000 and 20000) end sq
from part;


explain select max(p_size) > ( select count(*)-1 from part) from part;
select max(p_size) > ( select count(*)-1 from part) from part;

-- corr scalar query in project and scalar query in filter
explain select o.p_size, (select count(distinct p_type) from part p where p.p_partkey = o.p_partkey) tmp
    FROM part o right join (select * from part where p_size > (select avg(p_size) from part)) t on t.p_partkey = o.p_partkey;
select o.p_size, (select count(distinct p_type) from part p where p.p_partkey = o.p_partkey) tmp
    FROM part o right join (select * from part where p_size > (select avg(p_size) from part)) t on t.p_partkey = o.p_partkey;

-- multiple scalar queries in project
explain select (select max(p_size) from part), (select min(p_size) from part),
    (select avg(p_size) from part), (select sum(p_size) from part)
     from part;
select (select max(p_size) from part), (select min(p_size) from part),
    (select avg(p_size) from part), (select sum(p_size) from part)
     from part;

-- scalar subquery with join
explain select t1.p_size,
    (select count(*) from part p, part pp where p.p_size = pp.p_size and p.p_type = pp.p_type)
    from part t1;
select t1.p_size,
    (select count(*) from part p, part pp where p.p_size = pp.p_size and p.p_type = pp.p_type)
    from part t1;

-- scalar subquery in join and in filter
explain select t1.p_size,
    (select count(*) from part p, part pp where p.p_size = pp.p_size and p.p_type = pp.p_type
                                              and (select sum(p_size) from part a1 where a1.p_partkey = p.p_partkey
                                                                        group by a1.p_partkey) > 0)
    from part t1;
select t1.p_size,
    (select count(*) from part p, part pp where p.p_size = pp.p_size and p.p_type = pp.p_type
                                              and (select sum(p_size) from part a1 where a1.p_partkey = p.p_partkey
                                                                        group by a1.p_partkey) > 0)
    from part t1;

-- multiple scalar queries in projecdt with scalar query in filter
explain select t1.p_size,
    (select count(*) from part t2 where t2.p_partkey = t1.p_partkey group by t2.p_partkey),
    (select count(*) from part p, part pp where p.p_size = pp.p_size and p.p_type = pp.p_type
                                              and (select sum(p_size) from part a1 where a1.p_partkey = p.p_partkey
                                                                        group by a1.p_partkey) > 0)
    from part t1;
select t1.p_size,
    (select count(*) from part t2 where t2.p_partkey = t1.p_partkey group by t2.p_partkey),
    (select count(*) from part p, part pp where p.p_size = pp.p_size and p.p_type = pp.p_type
                                              and (select sum(p_size) from part a1 where a1.p_partkey = p.p_partkey
                                                                        group by a1.p_partkey) > 0)
    from part t1;

-- subquery in UDF
explain SELECT p_size, exp((SELECT max(p_size) FROM part p WHERE p.p_type = part.p_type)) from part;
SELECT p_size, exp((SELECT max(p_size) FROM part p WHERE p.p_type = part.p_type)) from part;
