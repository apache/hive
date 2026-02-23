CREATE TABLE test (col1 string, col2 string);

EXPLAIN CBO
SELECT col1, col2 FROM test
WHERE col2 = 'a'
DISTRIBUTE BY col1, col2; 
