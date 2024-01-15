set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.vectorized.execution.enabled=true;

create temporary table foo (col string) stored as orc;
create temporary table bar (col binary) stored as orc;

-- SORT_QUERY_RESULTS

INSERT INTO bar select unhex('6162636465-166676869');
INSERT INTO foo SELECT col FROM bar;

explain select col, count(*) from foo where col like '%fg%' group by col;
select col, count(*) from foo where col like '%fg%' group by col;

