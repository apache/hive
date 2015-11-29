set hive.optimize.ppd=true;

-- SORT_QUERY_RESULTS

drop table if exists union_all_bug_test_1;
drop table if exists union_all_bug_test_2;

create table if not exists union_all_bug_test_1
(
f1 int,
f2 int
);

create table if not exists union_all_bug_test_2
(
f1 int
);

explain SELECT f1
FROM (

SELECT
f1
, if('helloworld' like '%hello%' ,f1,f2) as filter
FROM union_all_bug_test_1

union all

select
f1
, 0 as filter
from union_all_bug_test_2
) A
WHERE (filter = 1);

SELECT f1
FROM (

SELECT
f1
, if('helloworld' like '%hello%' ,f1,f2) as filter
FROM union_all_bug_test_1

union all

select
f1
, 0 as filter
from union_all_bug_test_2
) A
WHERE (filter = 1);

insert into table union_all_bug_test_1 values (1,1);
insert into table union_all_bug_test_2 values (1);
insert into table union_all_bug_test_1 values (0,0);
insert into table union_all_bug_test_2 values (0);

SELECT f1
FROM (

SELECT
f1
, if('helloworld' like '%hello%' ,f1,f2) as filter
FROM union_all_bug_test_1

union all

select
f1
, 0 as filter
from union_all_bug_test_2
) A
WHERE (filter = 1);

SELECT f1
FROM (

SELECT
f1
, if('helloworld' like '%hello%' ,f1,f2) as filter
FROM union_all_bug_test_1

union all

select
f1
, 0 as filter
from union_all_bug_test_2
) A
WHERE (filter = 0);

SELECT f1
FROM (

SELECT
f1
, if('helloworld' like '%hello%' ,f1,f2) as filter
FROM union_all_bug_test_1

union all

select
f1
, 0 as filter
from union_all_bug_test_2
) A
WHERE (filter = 1 or filter = 0);

explain

SELECT f1
FROM (

SELECT
f1
, if('helloworld' like '%hello%' ,f1,f2) as filter
FROM union_all_bug_test_1

union all

select
f1
, 0 as filter
from union_all_bug_test_2
) A
WHERE (f1 = 1);

SELECT f1
FROM (

SELECT
f1
, if('helloworld' like '%hello%' ,f1,f2) as filter
FROM union_all_bug_test_1

union all

select
f1
, 0 as filter
from union_all_bug_test_2
) A
WHERE (f1 = 1);

drop table if exists map_json;
drop table if exists map_json1;
drop table if exists map_json2;

create table map_json1(
  id int,
  val array<string>);

create table map_json2(
  id int,
  val array<string>);

create table map_json(
  id int,
  val array<string>);

create view explode as
select id, l from map_json1 LATERAL VIEW explode(val) tup as l
UNION ALL
select id, get_json_object(l, '$.daysLeft') as l
from map_json2 LATERAL VIEW explode(val) tup as l
UNION ALL
select id, l from map_json LATERAL VIEW explode(val) elems as l;

select count(*) from explode where get_json_object(l, '$') is NOT NULL;

drop view explode;
drop table map_json;
drop table map_json1;
drop table map_json2;
SELECT f1
FROM (

SELECT
f1
, if('helloworld' like '%hello%' ,f1,f2) as filter
FROM union_all_bug_test_1

union all

select
f1
, 0 as filter
from union_all_bug_test_2
) A
WHERE (filter = 1 and f1 = 1);
