-- Verify that hive.optimize.cte.materialize.full.aggregate.only behaves as expected for implicitly discovered (hive.optimize.cte.suggester.type=CBO) CTEs in the query
CREATE TABLE emps
(
    empid  INTEGER,
    deptno INTEGER,
    name   VARCHAR(10),
    salary DECIMAL(8, 2)
);

set hive.optimize.cte.materialize.threshold=1;
set hive.optimize.cte.suggester.type=CBO;

set hive.optimize.cte.materialize.full.aggregate.only=true;

EXPLAIN CBO
SELECT COUNT(*) FROM emps e GROUP BY e.deptno
UNION
SELECT COUNT(*) FROM emps e GROUP BY e.deptno;

EXPLAIN CBO
SELECT COUNT(*), 'A' FROM emps e GROUP BY e.deptno
UNION
SELECT COUNT(*), 'B' FROM emps e GROUP BY e.deptno;

EXPLAIN CBO
SELECT name FROM emps e WHERE salary > 50000
UNION
SELECT name FROM emps e WHERE salary > 50000;

set hive.optimize.cte.materialize.full.aggregate.only=false;

EXPLAIN CBO
SELECT COUNT(*) FROM emps e GROUP BY e.deptno
UNION
SELECT COUNT(*) FROM emps e GROUP BY e.deptno;

EXPLAIN CBO
SELECT COUNT(*), 'A' FROM emps e GROUP BY e.deptno
UNION
SELECT COUNT(*), 'B' FROM emps e GROUP BY e.deptno;

EXPLAIN CBO
SELECT name FROM emps e WHERE salary > 50000
UNION
SELECT name FROM emps e WHERE salary > 50000;
