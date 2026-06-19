CREATE TABLE emps
(
    empid  INTEGER,
    deptno INTEGER,
    name   VARCHAR(10),
    salary DECIMAL(8, 2)
);

CREATE TABLE depts
(
    deptno INTEGER,
    name   VARCHAR(20)
);

set hive.optimize.cte.materialize.threshold=1;
set hive.optimize.cte.materialize.full.aggregate.only=false;

set hive.optimize.cte.suggester.type=AST;

EXPLAIN CBO
WITH dept_avg AS (
  SELECT d.name AS d_name, AVG(e.salary) AS avg_salary
  FROM emps e
  INNER JOIN depts d ON e.deptno = d.deptno
  GROUP BY d.name
)
SELECT d_name, 'HIGH'
FROM dept_avg da
WHERE da.avg_salary >= 100000
UNION
SELECT d_name, 'LOW'
FROM dept_avg da
WHERE da.avg_salary < 100000;

set hive.optimize.cte.suggester.type=CBO;

EXPLAIN CBO
WITH dept_avg AS (
  SELECT d.name AS d_name, AVG(e.salary) AS avg_salary
  FROM emps e
  INNER JOIN depts d ON e.deptno = d.deptno
  GROUP BY d.name
)
SELECT d_name, 'HIGH'
FROM dept_avg da
WHERE da.avg_salary >= 100000
UNION
SELECT d_name, 'LOW'
FROM dept_avg da
WHERE da.avg_salary < 100000;

set hive.optimize.cte.suggester.type=NONE;

EXPLAIN CBO
WITH dept_avg AS (
  SELECT d.name AS d_name, AVG(e.salary) AS avg_salary
  FROM emps e
  INNER JOIN depts d ON e.deptno = d.deptno
  GROUP BY d.name
)
SELECT d_name, 'HIGH'
FROM dept_avg da
WHERE da.avg_salary >= 100000
UNION
SELECT d_name, 'LOW'
FROM dept_avg da
WHERE da.avg_salary < 100000;
