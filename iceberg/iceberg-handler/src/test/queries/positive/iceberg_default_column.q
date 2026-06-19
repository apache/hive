CREATE TABLE t3 (
  id INT,
  point STRUCT<x:INT, y:INT> DEFAULT '{"x":100,"y":99}',
  name STRING DEFAULT 'unknown',
  age INT DEFAULT 25,
  salary DOUBLE DEFAULT 50000.0,
  is_active BOOLEAN DEFAULT TRUE,
  created_date DATE DEFAULT '2024-01-01',
  created_ts TIMESTAMP DEFAULT '2024-01-01T10:00:00',
  score DECIMAL(5,2) DEFAULT 100.00,
  category STRING DEFAULT 'general'
)
STORED BY ICEBERG
TBLPROPERTIES ('format-version'='3');

-- Case 1: Partial struct with explicit null field, the null should be preserved
INSERT INTO t3 (id, point) VALUES (2, named_struct('x', CAST(null AS INT), 'y', 7));

-- Case 2: Only ID specified (all defaults should apply)
INSERT INTO t3 (id) VALUES (3);

-- Case 3: Explicit NULL for a primitive field (should remain NULL, not get default)
INSERT INTO t3 (id, name) VALUES (4, NULL);

-- Case 4: Mixed scenario - some fields provided, some missing
INSERT INTO t3 (id, name, age) VALUES (5, 'custom_name', 30);

-- Case 5: Complex struct with nested nulls, the null should be preserved
INSERT INTO t3 (id, point) VALUES (6, named_struct('x', CAST(null AS INT), 'y', CAST(null AS INT)));

-- Case 6: ALTER AND SET NULL and 'null' string
ALTER TABLE t3 CHANGE COLUMN age age int DEFAULT null;
ALTER TABLE t3 CHANGE COLUMN name name string DEFAULT 'null';
ALTER TABLE t3 CHANGE COLUMN point point STRUCT<x:INT, y:INT> DEFAULT null;

INSERT INTO t3 (id) VALUES (7);

-- Case 7: Add a nested struct field with default value
ALTER TABLE t3 ADD COLUMNS (person STRUCT<
  name: STRING,
  address: STRUCT<
    street: STRING,
    city: STRING
  >
> DEFAULT '{"name":"John","address":{"street":"Main St","city":"New York"}}'
);

INSERT INTO t3 (id) VALUES (8);
-- Partial struct with explicit null field, the null should be preserved
INSERT INTO t3 (id, person)
VALUES (
  9,
  named_struct(
    'name', CAST(NULL AS STRING),
    'address', named_struct(
      'street', CAST(NULL AS STRING),
      'city', 'Bangalore'
    )
  )
);

SELECT * FROM t3 ORDER BY id;