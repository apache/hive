
SET hive.vectorized.execution.enabled=true;
SET hive.fetch.task.conversion=none;

CREATE TEMPORARY TABLE varchar_const_1 (c1 int) STORED AS ORC;

INSERT INTO varchar_const_1 values(42);

EXPLAIN
SELECT CONCAT(CAST('F' AS CHAR(2)), CAST('F' AS VARCHAR(2))) FROM VARCHAR_CONST_1;

SELECT CONCAT(CAST('F' AS CHAR(2)), CAST('F' AS VARCHAR(2))) FROM VARCHAR_CONST_1;

