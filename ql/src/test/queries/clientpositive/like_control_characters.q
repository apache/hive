set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.vectorized.execution.enabled=true;

create temporary table foo (col string);

-- SORT_QUERY_RESULTS

LOAD DATA LOCAL INPATH '../../data/files/control_characters.txt' INTO TABLE foo;

explain select col, count(*) from foo where col like '%fg%' group by col;
select col, count(*) from foo where col like '%fg%' group by col;

