-- SORT_QUERY_RESULTS
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

create table str_str_orc (str1 string, str2 string) stored as orc;

insert into table str_str_orc values (null, "X"), ("0", "X"), ("1", "X"), (null, "y");

-- Add a single NULL row that will come from ORC as isRepeated.
insert into str_str_orc values (NULL, NULL);

EXPLAIN VECTORIZATION EXPRESSION
SELECT
   str2, ROUND(sum(cast(COALESCE(str1, 0) as int))/60, 2) as result
from str_str_orc
GROUP BY str2;

SELECT
   str2, ROUND(sum(cast(COALESCE(str1, 0) as int))/60, 2) as result
from str_str_orc
GROUP BY str2;

EXPLAIN VECTORIZATION EXPRESSION
SELECT COALESCE(str1, 0) as result
from str_str_orc;

SELECT COALESCE(str1, 0) as result
from str_str_orc;

SET hive.vectorized.execution.enabled=true;

EXPLAIN VECTORIZATION EXPRESSION
SELECT
   str2, ROUND(sum(cast(COALESCE(str1, 0) as int))/60, 2) as result
from str_str_orc
GROUP BY str2;

SELECT
   str2, ROUND(sum(cast(COALESCE(str1, 0) as int))/60, 2) as result
from str_str_orc
GROUP BY str2;

EXPLAIN VECTORIZATION EXPRESSION
SELECT COALESCE(str1, 0) as result
from str_str_orc;

SELECT COALESCE(str1, 0) as result
from str_str_orc;
