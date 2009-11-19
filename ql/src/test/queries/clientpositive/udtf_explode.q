EXPLAIN EXTENDED SELECT explode(array(1,2,3)) AS myCol FROM src LIMIT 3;
EXPLAIN EXTENDED SELECT myCol, count(1) FROM (SELECT explode(array(1,2,3)) AS myCol FROM src LIMIT 3) a GROUP BY myCol;

SELECT explode(array(1,2,3)) AS myCol FROM src LIMIT 3;
SELECT myCol, count(1) FROM (SELECT explode(array(1,2,3)) AS myCol FROM src LIMIT 3) a GROUP BY myCol;
