--! qt:transactional
set hive.fetch.task.conversion=none;
set hive.strict.checks.type.safety=false;

CREATE TABLE test (a string, b float)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

INSERT INTO test VALUES ("a", 1);
INSERT INTO test VALUES ("b", 2);
INSERT INTO test VALUES ("c", 2);
INSERT INTO test VALUES ("d", 3);
INSERT INTO test VALUES ("e", null);

ANALYZE TABLE test COMPUTE STATISTICS FOR COLUMNS;

-- select ds_kll_stringify(ds_kll_sketch(cast(b as float))), ds_kll_quantile(ds_kll_sketch(CAST(b AS FLOAT)), 0.2) from test;
