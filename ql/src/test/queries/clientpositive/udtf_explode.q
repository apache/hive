EXPLAIN EXTENDED SELECT explode(array(1,2,3)) AS myCol FROM src LIMIT 3;
EXPLAIN EXTENDED SELECT a.myCol, count(1) FROM (SELECT explode(array(1,2,3)) AS myCol FROM src LIMIT 3) a GROUP BY a.myCol;

SELECT explode(array(1,2,3)) AS myCol FROM src LIMIT 3;
SELECT explode(array(1,2,3)) AS (myCol) FROM src LIMIT 3;
SELECT a.myCol, count(1) FROM (SELECT explode(array(1,2,3)) AS myCol FROM src LIMIT 3) a GROUP BY a.myCol;
