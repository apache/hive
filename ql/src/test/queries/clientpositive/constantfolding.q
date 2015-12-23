set hive.mapred.mode=nonstrict;
set hive.optimize.ppd=true;

-- SORT_QUERY_RESULTS

select * from (select 'k2' as key, '1 ' as value from src limit 2)b
union all 
select * from (select 'k3' as key, '' as value from src limit 2)b
union all 
select * from (select 'k4' as key, ' ' as value from src limit 2)c;
  

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
WHERE (filter = 1 and f1 = 1);


select percentile(cast(key as bigint), array()) from src where false;

select unbase64("0xe23") from src limit 1;

SELECT key,randum123, h4
FROM (SELECT *, cast(rand() as double) AS randum123, hex(4) AS h4 FROM src WHERE key = 100) a
WHERE a.h4 <= 3 limit 1;

select null from src limit 1;

-- numRows: 2 rawDataSize: 80
explain select cast("1970-12-31 15:59:58.174" as TIMESTAMP) from src;

-- numRows: 2 rawDataSize: 112
explain select cast("1970-12-31 15:59:58.174" as DATE) from src;

CREATE TABLE dest1(c1 STRING) STORED AS TEXTFILE;

FROM src INSERT OVERWRITE TABLE dest1 SELECT '  abc  ' WHERE src.key = 86;

EXPLAIN
SELECT ROUND(LN(3.0),12), LN(0.0), LN(-1), ROUND(LOG(3.0),12), LOG(0.0),
       LOG(-1), ROUND(LOG2(3.0),12), LOG2(0.0), LOG2(-1),
       ROUND(LOG10(3.0),12), LOG10(0.0), LOG10(-1), ROUND(LOG(2, 3.0),12),
       LOG(2, 0.0), LOG(2, -1), LOG(0.5, 2), LOG(2, 0.5), ROUND(EXP(2.0),12),
       POW(2,3), POWER(2,3), POWER(2,-3), POWER(0.5, -3), POWER(4, 0.5),
       POWER(-1, 0.5), POWER(-1, 2), POWER(CAST (1 AS DECIMAL), CAST (0 AS INT)),
       POWER(CAST (2 AS DECIMAL), CAST (3 AS INT)), 
       POW(CAST (2 AS DECIMAL), CAST(3 AS INT)) FROM dest1;

SELECT ROUND(LN(3.0),12), LN(0.0), LN(-1), ROUND(LOG(3.0),12), LOG(0.0),
       LOG(-1), ROUND(LOG2(3.0),12), LOG2(0.0), LOG2(-1),
       ROUND(LOG10(3.0),12), LOG10(0.0), LOG10(-1), ROUND(LOG(2, 3.0),12),
       LOG(2, 0.0), LOG(2, -1), LOG(0.5, 2), LOG(2, 0.5), ROUND(EXP(2.0),12),
       POW(2,3), POWER(2,3), POWER(2,-3), POWER(0.5, -3), POWER(4, 0.5),
       POWER(-1, 0.5), POWER(-1, 2), POWER(CAST (1 AS DECIMAL), CAST (0 AS INT)),
       POWER(CAST (2 AS DECIMAL), CAST (3 AS INT)), 
       POW(CAST (2 AS DECIMAL), CAST(3 AS INT)) FROM dest1;
