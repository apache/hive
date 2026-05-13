CREATE TABLE test (id string, items array<string>);
INSERT INTO test VALUES ('A', array('a', 'b')), ('B', array('c')), ('D', array());

CREATE VIEW v AS
SELECT test.id AS id, item
FROM test
LATERAL VIEW OUTER explode(test.items) lv AS item;

-- CBO plan should contain `outer=[true]` in HiveTableFunctionScan node.
EXPLAIN CBO
SELECT id, item FROM v ORDER BY id, item;
-- Explain plan should contain `outer lateral view: true` in the UDTF Operator
EXPLAIN
SELECT id, item FROM v ORDER BY id, item;
-- One of the output row should be ('D', NULL) since it's an outer lateral view.
SELECT id, item FROM v ORDER BY id, item;
