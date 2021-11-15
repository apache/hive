-- SORT_QUERY_RESULTS

describe function width_bucket;
desc function extended width_bucket;

explain select width_bucket(10, 5, 25, 4);

-- Test with constants
select
width_bucket(1, 5, 25, 4),
width_bucket(10, 5, 25, 4),
width_bucket(20, 5, 25, 4),
width_bucket(30, 5, 25, 4);

-- Test with NULLs
select
width_bucket(1, NULL, 25, 4),
width_bucket(NULL, 5, 25, 4),
width_bucket(20, 5, NULL, 4),
width_bucket(30, 5, 25, NULL),
width_bucket(NULL, NULL, NULL, NULL);

-- Test with negative values
select
width_bucket(-1, -25, -5, 4),
width_bucket(-10, -25, -5, 4),
width_bucket(-20, -25, -5, 4),
width_bucket(-30, -25, -5, 4);

-- Test with positive and negative values
select
width_bucket(-10, -5, 15, 4),
width_bucket(0, -5, 15, 4),
width_bucket(10, -5, 15, 4),
width_bucket(20, -5, 15, 4);

-- Test with decimals
select
width_bucket(0.1, 0, 1, 10),
width_bucket(0.25, 0, 1, 10),
width_bucket(0.3456, 0, 1, 10),
width_bucket(0.654321, 0, 1, 10);

-- Test with negative decimals
select
width_bucket(-0.5, -1.5, 1.5, 10),
width_bucket(-0.3, -1.5, 1.5, 10),
width_bucket(-0.25, -1.5, 1.5, 10),
width_bucket(0, -1.5, 1.5, 10),
width_bucket(0.75, -1.5, 1.5, 10),
width_bucket(1.25, -1.5, 1.5, 10),
width_bucket(1.5, -1.5, 1.5, 10);

-- Test with minValue > maxValue
select
width_bucket(1, 25, 5, 4),
width_bucket(10, 25, 5, 4),
width_bucket(20, 25, 5, 4),
width_bucket(30, 25, 5, 4);

-- Test with minValue > maxValue, with positive and negative values
select
width_bucket(-10, 15, -5, 4),
width_bucket(0, 15, -5, 4),
width_bucket(10, 15, -5, 4),
width_bucket(20, 15, -5, 4);

-- Test with minValue > maxValue, with decimals
select
width_bucket(0.1, 1, 0, 10),
width_bucket(0.25, 1, 0, 10),
width_bucket(0.3456, 1, 0, 10),
width_bucket(0.654321, 1, 0, 10);

-- Test with small decimal values
create table alldecimaltypes(
    cfloat FLOAT,
    cdouble DOUBLE);

insert into table alldecimaltypes values (0.1, 0.1), (0.25, 0.25), (0.3456, 0.3456), (0.654321, 0.654321);

select
width_bucket(cfloat, 0, 1, 10),
width_bucket(cdouble, 0, 1, 10)
from alldecimaltypes;

select
width_bucket(cfloat, 0, 1.5, 10),
width_bucket(cdouble, -1.5, 0, 10),
width_bucket(0.25, cfloat, 2, 10),
width_bucket(0.25, 0, cdouble, 10)
from alldecimaltypes;

-- Test with all numeric types
create table alltypes_n3(
    ctinyint TINYINT,
    csmallint SMALLINT,
    cint INT,
    cbigint BIGINT,
    cfloat FLOAT,
    cdouble DOUBLE);

insert into table alltypes_n3 values
(0, 0, 0, 0, 0.0, 0.0),
(1, 1, 1, 1, 1.0, 1.0),
(25, 25, 25, 25, 25.0, 25.0),
(60, 60, 60, 60, 60.0, 60.0),
(72, 72, 72, 72, 72.0, 72.0),
(100, 100, 100, 100, 100.0, 100.0);

-- Test each numeric type individually
select
width_bucket(ctinyint, 0, 100, 10),
width_bucket(csmallint, 0, 100, 10),
width_bucket(cint, 0, 100, 10),
width_bucket(cbigint, 0, 100, 10),
width_bucket(cfloat, 0, 100, 10),
width_bucket(cdouble, 0, 100, 10)
from alltypes_n3;

truncate table alltypes_n3;

insert into table alltypes_n3 values (5, 5, 5, 10, 4.5, 7.25);

-- Test different numeric types in a single query
select
width_bucket(cdouble, ctinyint, cbigint, 10),
width_bucket(cdouble, csmallint, cbigint, 10),
width_bucket(cdouble, cint, cbigint, 10),
width_bucket(cdouble, cfloat, cbigint, 10)
from alltypes_n3;

-- Test all tinyints
create table alltinyints (
    ctinyint1 TINYINT,
    ctinyint2 TINYINT,
    ctinyint3 TINYINT,
    cint INT);

insert into table alltinyints values (5, 1, 10, 2);
select width_bucket(ctinyint1, ctinyint2, ctinyint3, cint) from alltinyints;

-- Test all smallints
create table allsmallints (
    csmallint1 SMALLINT,
    csmallint2 SMALLINT,
    csmallint3 SMALLINT,
    cint INT);

insert into table allsmallints values (5, 1, 10, 2);
select width_bucket(csmallint1, csmallint2, csmallint3, cint) from allsmallints;

-- Test all ints
create table allints (
    cint1 INT,
    cint2 INT,
    cint3 INT,
    cint4 INT);

insert into table allints values (5, 1, 10, 2);
select width_bucket(cint1, cint2, cint3, cint4) from allints;

-- Test all bigints
create table allbigints (
    cbigint1 BIGINT,
    cbigint2 BIGINT,
    cbigint3 BIGINT,
    cint INT);

insert into table allbigints values (5, 1, 10, 2);
select width_bucket(cbigint1, cbigint2, cbigint3, cint) from allbigints;

-- Test all floats
create table allfloats (
    cfloat1 FLOAT,
    cfloat2 FLOAT,
    cfloat3 FLOAT,
    cint INT);

insert into table allfloats values (5.0, 1.0, 10.0, 2);
select width_bucket(cfloat1, cfloat2, cfloat3, cint) from allfloats;

-- Test all doubles
create table alldoubles (
    cdouble1 DOUBLE,
    cdouble2 DOUBLE,
    cdouble3 DOUBLE,
    cint INT);

insert into table alldoubles values (5.0, 1.0, 10.0, 2);
select width_bucket(cdouble1, cdouble2, cdouble3, cint) from alldoubles;

-- Test with grouping sets
create table testgroupingsets (c1 int, c2 int);
insert into table testgroupingsets values (1, 1), (2, 2);
select c1, c2, width_bucket(5, c1, 10, case when grouping(c2) = 0 then 10 else 5 end) from testgroupingsets group by cube(c1, c2);

drop table alldecimaltype;
drop table alltypes_n3;
drop table alltinyints;
drop table allsmallints;
drop table allints;
drop table allbigints;
drop table allfloats;
drop table alldoubles;
drop table testgroupingsets;
