--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.fetch.task.conversion=more;

describe function between;
describe function extended between;

explain SELECT * FROM src where key + 100 between (150 + -50) AND (150 + 50) LIMIT 20;
SELECT * FROM src where key + 100 between (150 + -50) AND (150 + 50) LIMIT 20;

explain SELECT * FROM src where key + 100 not between (150 + -50) AND (150 + 50) LIMIT 20;
SELECT * FROM src where key + 100 not between (150 + -50) AND (150 + 50) LIMIT 20;

explain SELECT * FROM src where 'b' between 'a' AND 'c' LIMIT 1;
SELECT * FROM src where 'b' between 'a' AND 'c' LIMIT 1;

explain SELECT * FROM src where 2 between 2 AND '3' LIMIT 1;
SELECT * FROM src where 2 between 2 AND '3' LIMIT 1;


create table t(i int);
insert into t values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11);

SELECT * FROM t	where	i between 8 and 9
		or	i between 9 and 10;

explain
SELECT * FROM t	where	i between 8 and 9
		or	i between 9 and 10;

SELECT * FROM t	where	i between 8 and 9
		or	i between 9 and 10;

explain
SELECT * FROM t	where	i between 6 and 7
		or	i between 9 and 10;

SELECT * FROM t	where	i between 6 and 7
		or	i between 9 and 10;

explain
SELECT * FROM t	where	i not between 6 and 7 
		and	i not between 9 and 10;

SELECT * FROM t	where	i not between 6 and 7 
		and	i not between 9 and 10;

