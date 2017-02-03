set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

EXPLAIN VECTORIZATION EXPRESSION SELECT (ctinyint % 2) + 1, cstring1, cint, elt((ctinyint % 2) + 1, cstring1, cint) 
FROM alltypesorc
WHERE ctinyint > 0 LIMIT 10;

SELECT (ctinyint % 2) + 1, cstring1, cint, elt((ctinyint % 2) + 1, cstring1, cint) 
FROM alltypesorc
WHERE ctinyint > 0 LIMIT 10;

EXPLAIN VECTORIZATION EXPRESSION
SELECT elt(2, 'abc', 'defg'),
       elt(3, 'aa', 'bb', 'cc', 'dd', 'ee', 'ff', 'gg'),
       elt('1', 'abc', 'defg'),
       elt(2, 'aa', CAST('2' AS TINYINT)),
       elt(2, 'aa', CAST('12345' AS SMALLINT)),
       elt(2, 'aa', CAST('123456789012' AS BIGINT)),
       elt(2, 'aa', CAST(1.25 AS FLOAT)),
       elt(2, 'aa', CAST(16.0 AS DOUBLE)),
       elt(0, 'abc', 'defg'),
       elt(3, 'abc', 'defg')
FROM alltypesorc LIMIT 1;

SELECT elt(2, 'abc', 'defg'),
       elt(3, 'aa', 'bb', 'cc', 'dd', 'ee', 'ff', 'gg'),
       elt('1', 'abc', 'defg'),
       elt(2, 'aa', CAST('2' AS TINYINT)),
       elt(2, 'aa', CAST('12345' AS SMALLINT)),
       elt(2, 'aa', CAST('123456789012' AS BIGINT)),
       elt(2, 'aa', CAST(1.25 AS FLOAT)),
       elt(2, 'aa', CAST(16.0 AS DOUBLE)),
       elt(0, 'abc', 'defg'),
       elt(3, 'abc', 'defg')
FROM alltypesorc LIMIT 1;
