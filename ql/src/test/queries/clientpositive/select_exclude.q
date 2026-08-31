set hive.cli.print.header=true;

CREATE TABLE test_exclude (
  id INT,
  name STRING,
  email STRING,
  address STRING,
  phone STRING
);

INSERT INTO test_exclude VALUES
(1, 'Alice', 'alice@test.com', '123 Apple St', '555-0100'),
(2, 'Bob', 'bob@test.com', '456 Banana Ave', '555-0200');

-- Exclude a single column
EXPLAIN SELECT * EXCLUDE (email) FROM test_exclude;
SELECT * EXCLUDE (email) FROM test_exclude;

-- Exclude multiple columns
EXPLAIN SELECT * EXCLUDE (email, address, phone) FROM test_exclude;
SELECT * EXCLUDE (email, address, phone) FROM test_exclude;

-- Exclude with table alias
EXPLAIN SELECT t.* EXCLUDE (id, phone) FROM test_exclude t;
SELECT t.* EXCLUDE (id, phone) FROM test_exclude t;

-- Exclude with JOIN
CREATE TABLE test_exclude_join (
  id INT,
  department STRING
);
INSERT INTO test_exclude_join VALUES (1, 'Engineering'), (2, 'Sales');

EXPLAIN
SELECT a.* EXCLUDE (address, phone), b.* EXCLUDE (id)
FROM test_exclude a JOIN test_exclude_join b ON a.id = b.id;

SELECT a.* EXCLUDE (address, phone), b.* EXCLUDE (id)
FROM test_exclude a JOIN test_exclude_join b ON a.id = b.id;
