CREATE TABLE emps
(
    empid  INTEGER,
    deptno INTEGER,
    name   VARCHAR(10),
    salary DECIMAL(8, 2)
);

set hive.optimize.cte.materialize.threshold=1;
set hive.optimize.cte.suggester.class=org.apache.hadoop.hive.ql.optimizer.calcite.CommonTableExpressionIdentitySuggester;
set hive.optimize.cte.materialize.full.aggregate.only=false;

EXPLAIN FORMATTED CBO
SELECT name FROM emps e WHERE salary > 50000
UNION
SELECT name FROM emps e WHERE salary > 50000;
