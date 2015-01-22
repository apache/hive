create table t(category int, live int, comments int);
insert into table t select key, 0, 2 from src tablesample(3 rows);

explain
select category, max(live) live, max(comments) comments, rank() OVER (PARTITION BY category ORDER BY comments) rank1
FROM t
GROUP BY category
GROUPING SETS ((), (category))
HAVING max(comments) > 0;

select category, max(live) live, max(comments) comments, rank() OVER (PARTITION BY category ORDER BY comments) rank1
FROM t
GROUP BY category
GROUPING SETS ((), (category))
HAVING max(comments) > 0;
