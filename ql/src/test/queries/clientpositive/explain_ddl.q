-- This test is used for testing explain for DDL/DML statements

-- Create some views and tabels
CREATE VIEW V1 AS SELECT key, value from src;
select count(*) from V1 where key > 0;

CREATE TABLE M1 AS SELECT key, value from src;
select count(*) from M1 where key > 0;

EXPLAIN CREATE TABLE M1 AS select * from src;
EXPLAIN CREATE TABLE M1 AS select * from M1;
EXPLAIN CREATE TABLE M1 AS select * from V1;

EXPLAIN CREATE TABLE V1 AS select * from M1;
EXPLAIN CREATE VIEW V1 AS select * from M1;

EXPLAIN CREATE TABLE M1 LIKE src;
EXPLAIN CREATE TABLE M1 LIKE M1;

EXPLAIN DROP TABLE M1;
select count(*) from M1 where key > 0;

EXPLAIN INSERT INTO M1 SELECT * FROM M1;
select count(*) from M1 where key > 0;

EXPLAIN TRUNCATE TABLE M1;
select count(*) from M1 where key > 0;

