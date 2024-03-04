CREATE TABLE emps
(
    empid  INTEGER,
    deptno INTEGER,
    name   VARCHAR(10),
    salary DECIMAL(8, 2)
);

INSERT INTO emps VALUES (0, 0, 'Rob', 150000.50), (1, 0, 'Alice', 160000.30), (2, 0, 'Mark', 100000.20), 
                        (3, 1, 'Greg', 50000.20), (4, 1, 'Josh', 150000.50), (5, 1, 'Hector', 50000.20);

CREATE TABLE depts
(
    deptno INTEGER,
    name   VARCHAR(20)
);

INSERT INTO depts VALUES (0, 'Engineering'), (1, 'Support'), (2, 'Sales');
-- Find employees of the engineering and support department which have the same salary

set hive.cbo.returnpath.hiveop=true;

EXPLAIN SELECT sup.name, eng.name
FROM (SELECT e.name, e.salary
      FROM emps e
      INNER JOIN depts d ON e.deptno = d.deptno AND d.name = 'Engineering') eng,
     (SELECT e.name, e.salary
      FROM emps e
      INNER JOIN depts d ON e.deptno = d.deptno AND d.name = 'Support') sup
WHERE sup.salary = eng.salary;
