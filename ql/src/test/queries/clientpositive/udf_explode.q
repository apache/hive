--! qt:dataset:src
set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION explode;
DESCRIBE FUNCTION EXTENDED explode;

EXPLAIN EXTENDED SELECT explode(array(1, 2, 3)) AS myCol FROM src tablesample (1 rows) ORDER BY myCol;
EXPLAIN EXTENDED SELECT a.myCol, count(1) FROM (SELECT explode(array(1, 2, 3)) AS myCol FROM src tablesample (1 rows)) a GROUP BY a.myCol ORDER BY a.myCol;

SELECT explode(array(1, 2, 3)) AS myCol FROM src tablesample (1 rows) ORDER BY myCol;
SELECT explode(array(1, 2, 3)) AS (myCol) FROM src tablesample (1 rows) ORDER BY myCol;
SELECT a.myCol, count(1) FROM (SELECT explode(array(1, 2, 3)) AS myCol FROM src tablesample (1 rows)) a GROUP BY a.myCol ORDER BY a.myCol;

EXPLAIN EXTENDED SELECT explode(map(1, 'one', 2, 'two', 3, 'three')) AS (key, val) FROM src tablesample (1 rows) ORDER BY key, val;
EXPLAIN EXTENDED SELECT a.key, a.val, count(1) FROM (SELECT explode(map(1, 'one', 2, 'two', 3, 'three')) AS (key, val) FROM src tablesample (1 rows) ORDER BY key, value) a GROUP BY a.key, a.val ORDER BY a.key, a.val;

SELECT explode(map(1, 'one', 2, 'two', 3, 'three')) AS (key, val) FROM src tablesample (1 rows) ORDER BY key, val;
SELECT a.key, a.val, count(1) FROM (SELECT explode(map(1, 'one', 2, 'two', 3, 'three')) AS (key, val) FROM src tablesample (1 rows) ORDER BY key, val) a GROUP BY a.key, a.val ORDER BY a.key, a.val;

drop table lazy_array_map;
create table lazy_array_map (map_col map<int,string>, array_col array<string>);
INSERT OVERWRITE TABLE lazy_array_map select map(1, 'one', 2, 'two', 3, 'three'), array('100', '200', '300') FROM src tablesample (1 rows);

SELECT array_col, myCol FROM lazy_array_map lateral view explode(array_col) X AS myCol ORDER BY array_col, myCol;
SELECT map_col, myKey, myValue FROM lazy_array_map lateral view explode(map_col) X AS myKey, myValue ORDER BY map_col, myKey, myValue;

create table source1 (dt string, d1 int, d2 int) stored as orc;
create table source2 (dt string, d1 int, d2 int) stored as orc;
insert into source1 values ('20211107', 1, 2);
insert into source2 values ('20211108', 11, 22);
select explode(map('D219', d1,'D220', d2)) as (keyx, valuex) from source1 union all select explode(map('D221', d1,'D222', d2)) as (keyy, valuey) from source2;
