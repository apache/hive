CREATE TABLE emps
(
    empid  INTEGER,
    deptno INTEGER,
    name   VARCHAR(10),
    salary DECIMAL(8, 2)
);

INSERT INTO emps VALUES (0, 0, 'Rob', 100000), (1, 0, 'Alice', 110000), (2, 0, 'Mark', 120000),
                        (3, 1, 'Greg', 80000), (4, 1, 'Josh', 90000), (5, 1, 'Hector', 95000),
                        (6, 2, 'Fred', 200000), (7, 2, 'Todd', 250000);

CREATE TABLE depts
(
    deptno INTEGER,
    name   VARCHAR(20)
);

INSERT INTO depts VALUES (0, 'Engineering'), (1, 'Support'), (2, 'Sales');

-- Classify and return departments (the names) based on the average employee salary (HIGH > 100K, LOW < 100K)
SELECT d.name, 'HIGH'
FROM emps e
INNER JOIN depts d ON e.deptno = d.deptno
GROUP BY d.name
HAVING AVG(e.salary) >= 100000
UNION
SELECT d.name, 'LOW'
FROM emps e
INNER JOIN depts d ON e.deptno = d.deptno
GROUP BY d.name
HAVING AVG(e.salary) < 100000;

EXPLAIN
SELECT d.name, 'HIGH'
FROM emps e
INNER JOIN depts d ON e.deptno = d.deptno
GROUP BY d.name
HAVING AVG(e.salary) >= 100000
UNION
SELECT d.name, 'LOW'
FROM emps e
INNER JOIN depts d ON e.deptno = d.deptno
GROUP BY d.name
HAVING AVG(e.salary) < 100000;

EXPLAIN CBO
SELECT d.name, 'HIGH'
FROM emps e
INNER JOIN depts d ON e.deptno = d.deptno
GROUP BY d.name
HAVING AVG(e.salary) >= 100000
UNION
SELECT d.name, 'LOW'
FROM emps e
INNER JOIN depts d ON e.deptno = d.deptno
GROUP BY d.name
HAVING AVG(e.salary) < 100000;

set hive.optimize.cte.materialize.threshold=1;
set hive.optimize.cte.materialize.full.aggregate.only=false;
set hive.optimize.cte.suggester.type=CBO;

SELECT d.name, 'HIGH'
FROM emps e
INNER JOIN depts d ON e.deptno = d.deptno
GROUP BY d.name
HAVING AVG(e.salary) >= 100000
UNION
SELECT d.name, 'LOW'
FROM emps e
INNER JOIN depts d ON e.deptno = d.deptno
GROUP BY d.name
HAVING AVG(e.salary) < 100000;

EXPLAIN
SELECT d.name, 'HIGH'
FROM emps e
INNER JOIN depts d ON e.deptno = d.deptno
GROUP BY d.name
HAVING AVG(e.salary) >= 100000
UNION
SELECT d.name, 'LOW'
FROM emps e
INNER JOIN depts d ON e.deptno = d.deptno
GROUP BY d.name
HAVING AVG(e.salary) < 100000;

EXPLAIN CBO
SELECT d.name, 'HIGH'
FROM emps e
INNER JOIN depts d ON e.deptno = d.deptno
GROUP BY d.name
HAVING AVG(e.salary) >= 100000
UNION
SELECT d.name, 'LOW'
FROM emps e
INNER JOIN depts d ON e.deptno = d.deptno
GROUP BY d.name
HAVING AVG(e.salary) < 100000;

-- Ensure that branch pruning (e.q., in UNION) is done before CTE detection and does not lead to broken queries/plans.
-- A scan over a CTE should always have the respective Spool operator in the plan; in other words if we introduce CTEs
-- we should not prune out the Spools.
EXPLAIN CBO
SELECT * FROM (
SELECT d.name, 'HIGH' as salary_range
FROM emps e
INNER JOIN depts d ON e.deptno = d.deptno
GROUP BY d.name
HAVING AVG(e.salary) >= 100000
UNION
SELECT d.name, 'LOW' as salary_range
FROM emps e
INNER JOIN depts d ON e.deptno = d.deptno
GROUP BY d.name
HAVING AVG(e.salary) < 100000) summary
WHERE salary_range = 'HIGH';

SELECT * FROM (
SELECT d.name, 'HIGH' as salary_range
FROM emps e
INNER JOIN depts d ON e.deptno = d.deptno
GROUP BY d.name
HAVING AVG(e.salary) >= 100000
UNION
SELECT d.name, 'LOW' as salary_range
FROM emps e
INNER JOIN depts d ON e.deptno = d.deptno
GROUP BY d.name
HAVING AVG(e.salary) < 100000) summary
WHERE salary_range = 'HIGH';

EXPLAIN CBO
SELECT * FROM (
SELECT d.name, 'HIGH' as salary_range
FROM emps e
INNER JOIN depts d ON e.deptno = d.deptno
GROUP BY d.name
HAVING AVG(e.salary) >= 100000
UNION
SELECT d.name, 'LOW' as salary_range
FROM emps e
INNER JOIN depts d ON e.deptno = d.deptno
GROUP BY d.name
HAVING AVG(e.salary) < 100000) summary
WHERE salary_range = 'LOW';

SELECT * FROM (
SELECT d.name, 'HIGH' as salary_range
FROM emps e
INNER JOIN depts d ON e.deptno = d.deptno
GROUP BY d.name
HAVING AVG(e.salary) >= 100000
UNION
SELECT d.name, 'LOW' as salary_range
FROM emps e
INNER JOIN depts d ON e.deptno = d.deptno
GROUP BY d.name
HAVING AVG(e.salary) < 100000) summary
WHERE salary_range = 'LOW';