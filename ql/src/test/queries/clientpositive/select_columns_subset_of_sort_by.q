CREATE TABLE test (
    col1 STRING,
    col2 STRING
);

EXPLAIN CBO
SELECT col1
FROM test
SORT BY col1, col2;
