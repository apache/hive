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

SELECT grouping(category), lead(live) over(partition by grouping(category))
FROM t
GROUP BY category, live
GROUPING SETS ((), (category));

SELECT grouping(category), lead(live) over(partition by grouping(category))
FROM t
GROUP BY category, live;

SELECT grouping(category), lag(live) over(partition by grouping(category))
FROM t
GROUP BY category, live;
