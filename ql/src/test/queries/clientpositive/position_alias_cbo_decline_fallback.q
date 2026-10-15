-- SORT_QUERY_RESULTS
-- HIVE-30037: with a fallback strategy other than NEVER, an invalid position makes CBO recompile the statement
-- without CBO, which ignores positions in SORT BY, DISTRIBUTE BY and CLUSTER BY. A statement that CBO declines
-- is recompiled the same way.

create table pacdf_t (d int, s string);
insert into pacdf_t values (1, 'a'), (3, 'c'), (2, 'b');

set hive.cbo.fallback.strategy=CONSERVATIVE;
-- the recompiled plans ignore every position, the valid one included: no sort key, a constant partition key
explain select d, s from pacdf_t sort by 2 desc, 9;
explain select d, s from pacdf_t tablesample (5 rows) sort by 2 desc, 9;
explain select d, s from pacdf_t distribute by 0 sort by 2 desc;
explain select d, s from pacdf_t tablesample (5 rows) distribute by 0 sort by 2 desc;
-- control: with valid positions only, the declined statement keeps its keys
explain select d, s from pacdf_t tablesample (5 rows) distribute by 1 sort by 2 desc;
select d, s from pacdf_t sort by 2 desc, 9;
select d, s from pacdf_t tablesample (5 rows) sort by 2 desc, 9;
select d, s from pacdf_t tablesample (5 rows) distribute by 0;

set hive.cbo.fallback.strategy=ALWAYS;
select d, s from pacdf_t tablesample (5 rows) cluster by 9;
select d, s from pacdf_t tablesample (5 rows) sort by 1.5;
