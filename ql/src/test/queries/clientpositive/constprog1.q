--! qt:dataset:src
set hive.fetch.task.conversion=more;
set hive.optimize.constant.propagation=true;

EXPLAIN
SELECT IF(INSTR(CONCAT('foo', 'bar'), 'foob') > 0, "F1", "B1")
       FROM src tablesample (1 rows);

SELECT IF(INSTR(CONCAT('foo', 'bar'), 'foob') > 0, "F1", "B1")
       FROM src tablesample (1 rows);
