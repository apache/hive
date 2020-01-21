
set hive.vectorized.execution.enabled=true;
set hive.cbo.enable=true;
set hive.stats.column.autogather=true;
set hive.auto.convert.join.noconditionaltask.size=10000000;
set hive.auto.convert.join=true;

create temporary table table_19 (decimal0801_col decimal(8,1), int_col_1 int) stored as orc;
create temporary table table_6 (int_col_0 int) stored as orc;
CREATE TEMPORARY TABLE table_27 (t_id STRING, f_id STRING, type STRING) stored as orc;

insert into table_19 values 
(418.9,	1000),
(418.9,	-759),
(418.9,	-663),
(418.9,	NULL),
(418.9,	-959);

insert into table_6 values (1000);

INSERT INTO table_27 values
('A','F','TEST'),('B','F','TEST'),('C','F','TEST'),('D','F','TEST'),('E','F','TEST'),
('F','F','TEST'),('G','F','TEST'),('H','F','TEST'),('I','F','TEST'),('J','F','TEST');

SELECT t1.decimal0801_col
FROM table_19 t1
WHERE (SELECT max(tt1.int_col_0) AS int_col FROM table_6 tt1) IN (t1.int_col_1) AND decimal0801_col is not null;


SELECT t1.decimal0801_col
FROM table_19 t1
WHERE (t1.int_col_1) IN (SELECT max(tt1.int_col_0) AS int_col FROM table_6 tt1) AND decimal0801_col is not null;


SELECT t1.decimal0801_col
FROM table_19 t1
WHERE (SELECT max(tt1.int_col_0) AS int_col FROM table_6 tt1) = (t1.int_col_1) AND decimal0801_col is not null;

SELECT
  t1.t_id,
  null
  FROM table_27 t1
  JOIN table_27 t2 ON (t1.t_id = t2.t_id and t1.f_id = t2.f_id)
  JOIN
  (SELECT 'TEST-1' id, 'TEST' r_type UNION ALL SELECT 'TEST-2' id, 'TEST' r_type) t3
  ON (t3.r_type = t1.type and t3.id in ('TEST-1', 'TEST-2'))
  ORDER BY t1.t_id ASC;

set hive.explain.user=false;

EXPLAIN VECTORIZATION DETAIL
SELECT t1.decimal0801_col
FROM table_19 t1
WHERE (SELECT max(tt1.int_col_0) AS int_col FROM table_6 tt1) IN (t1.int_col_1) AND decimal0801_col is not null;


EXPLAIN VECTORIZATION DETAIL
SELECT t1.decimal0801_col
FROM table_19 t1
WHERE (t1.int_col_1) IN (SELECT max(tt1.int_col_0) AS int_col FROM table_6 tt1) AND decimal0801_col is not null;


EXPLAIN VECTORIZATION DETAIL
SELECT t1.decimal0801_col
FROM table_19 t1
WHERE (SELECT max(tt1.int_col_0) AS int_col FROM table_6 tt1) = (t1.int_col_1) AND decimal0801_col is not null;

EXPLAIN VECTORIZATION DETAIL
SELECT
  t1.t_id,
  null
  FROM table_27 t1
  JOIN table_27 t2 ON (t1.t_id = t2.t_id and t1.f_id = t2.f_id)
  JOIN
  (SELECT 'TEST-1' id, 'TEST' r_type UNION ALL SELECT 'TEST-2' id, 'TEST' r_type) t3
  ON (t3.r_type = t1.type and t3.id in ('TEST-1', 'TEST-2'));
