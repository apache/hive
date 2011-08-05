DESCRIBE FUNCTION explode;
DESCRIBE FUNCTION EXTENDED explode;

EXPLAIN EXTENDED SELECT explode(array(1,2,3)) AS myCol FROM src LIMIT 3;
EXPLAIN EXTENDED SELECT a.myCol, count(1) FROM (SELECT explode(array(1,2,3)) AS myCol FROM src LIMIT 3) a GROUP BY a.myCol;

SELECT explode(array(1,2,3)) AS myCol FROM src LIMIT 3;
SELECT explode(array(1,2,3)) AS (myCol) FROM src LIMIT 3;
SELECT a.myCol, count(1) FROM (SELECT explode(array(1,2,3)) AS myCol FROM src LIMIT 3) a GROUP BY a.myCol;

EXPLAIN EXTENDED SELECT explode(map(1,'one',2,'two',3,'three')) AS (key,val) FROM src LIMIT 3;
EXPLAIN EXTENDED SELECT a.key, a.val, count(1) FROM (SELECT explode(map(1,'one',2,'two',3,'three')) AS (key,val) FROM src LIMIT 3) a GROUP BY a.key, a.val;

SELECT explode(map(1,'one',2,'two',3,'three')) AS (key,val) FROM src LIMIT 3;
SELECT a.key, a.val, count(1) FROM (SELECT explode(map(1,'one',2,'two',3,'three')) AS (key,val) FROM src LIMIT 3) a GROUP BY a.key, a.val;