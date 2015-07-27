set hive.optimize.ppd=true;
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
