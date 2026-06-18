-- Tests for nested IS NULL / IS NOT NULL predicates.
-- (name IS NULL) IS NULL should simplify to FALSE since IS NULL always returns a non-nullable BOOLEAN.
-- (name IS NOT NULL) IS NULL should simplify to FALSE for the same reason.

CREATE TABLE t_is_null_nested (id INT, name STRING);

INSERT INTO t_is_null_nested VALUES
  (1, 'alice'),
  (2, NULL);

-- always FALSE
SELECT id, (name IS NULL) IS NULL FROM t_is_null_nested ORDER BY id;

-- always FALSE
SELECT id, (name IS NOT NULL) IS NULL FROM t_is_null_nested ORDER BY id;

-- always TRUE
SELECT id, (name IS NULL) IS NOT NULL FROM t_is_null_nested ORDER BY id;

-- always TRUE
SELECT id, (name IS NOT NULL) IS NOT NULL FROM t_is_null_nested ORDER BY id;

-- no rows should be filtered out
SELECT id FROM t_is_null_nested WHERE (name IS NULL) IS NOT NULL ORDER BY id;

-- should filter out all rows
SELECT id FROM t_is_null_nested WHERE (name IS NULL) IS NULL ORDER BY id;

