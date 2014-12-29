SET hive.vectorized.execution.enabled=false;
set hive.fetch.task.conversion=none;

create table str_str_orc (str1 string, str2 string) stored as orc;

insert into table str_str_orc values (null, "X"), ("0", "X"), ("1", "X"), (null, "y");

EXPLAIN
SELECT
   str2, ROUND(sum(cast(COALESCE(str1, 0) as int))/60, 2) as result
from str_str_orc
GROUP BY str2;

SELECT
   str2, ROUND(sum(cast(COALESCE(str1, 0) as int))/60, 2) as result
from str_str_orc
GROUP BY str2;

EXPLAIN
SELECT COALESCE(str1, 0) as result
from str_str_orc;

SELECT COALESCE(str1, 0) as result
from str_str_orc;

SET hive.vectorized.execution.enabled=true;

EXPLAIN
SELECT
   str2, ROUND(sum(cast(COALESCE(str1, 0) as int))/60, 2) as result
from str_str_orc
GROUP BY str2;

SELECT
   str2, ROUND(sum(cast(COALESCE(str1, 0) as int))/60, 2) as result
from str_str_orc
GROUP BY str2;

EXPLAIN
SELECT COALESCE(str1, 0) as result
from str_str_orc;

SELECT COALESCE(str1, 0) as result
from str_str_orc;
