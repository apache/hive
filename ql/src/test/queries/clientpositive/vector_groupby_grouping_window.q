--! qt:dataset:src
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.execution.ptf.enabled=true;
set hive.fetch.task.conversion=none;
set hive.cli.print.header=true;

create table t_n15(category int, live int, comments int) stored as orc;
insert into table t_n15 select key, 0, 2 from src tablesample(3 rows);

explain vectorization detail
select category, max(live) live, max(comments) comments, rank() OVER (PARTITION BY category ORDER BY comments) rank1
FROM t_n15
GROUP BY category
GROUPING SETS ((), (category))
HAVING max(comments) > 0;

select category, max(live) live, max(comments) comments, rank() OVER (PARTITION BY category ORDER BY comments) rank1
FROM t_n15
GROUP BY category
GROUPING SETS ((), (category))
HAVING max(comments) > 0;
