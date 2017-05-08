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

-- SCALAR, corr
explain SELECT p_size, (SELECT max(p_size) FROM part p WHERE p.p_type = part.p_type)
FROM part;

SELECT p_size, (SELECT max(p_size) FROM part p WHERE p.p_type = part.p_type)
FROM part;

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
