set hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=more;
set hive.cbo.enable=true;
set hive.query.results.cache.enabled=false;
CREATE TABLE testjfs3(i int);
INSERT INTO testjfs3 values (-1591211872);
ALTER TABLE testjfs3 change column i i float;
set hive.cbo.enable=false;
EXPLAIN
SELECT * FROM testjfs3 WHERE i = -1591211872;
SELECT * FROM testjfs3 WHERE i = -1591211872;
set hive.cbo.enable=true;
EXPLAIN
SELECT * FROM testjfs3 WHERE i = -1591211872;
SELECT * FROM testjfs3 WHERE i = -1591211872;
