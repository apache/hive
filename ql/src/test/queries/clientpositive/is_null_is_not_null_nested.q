CREATE TABLE test (id INT, name STRING);

-- always FALSE
EXPLAIN CBO SELECT id, (name IS NULL) IS NULL FROM test;

-- always FALSE
EXPLAIN CBO SELECT id, (name IS NOT NULL) IS NULL FROM test;

-- always TRUE
EXPLAIN CBO SELECT id, (name IS NULL) IS NOT NULL FROM test;

-- always TRUE
EXPLAIN CBO SELECT id, (name IS NOT NULL) IS NOT NULL FROM test;

-- always-true condition: no rows filtered
EXPLAIN CBO SELECT id FROM test WHERE (name IS NULL) IS NOT NULL;

-- always-false condition: all rows filtered
EXPLAIN CBO SELECT id FROM test WHERE (name IS NULL) IS NULL;
