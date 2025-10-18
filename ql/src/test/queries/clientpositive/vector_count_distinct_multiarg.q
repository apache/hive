drop table if exists test_vector;
create external table test_vector(id string, pid bigint) PARTITIONED BY (full_date int);
insert into test_vector (pid, full_date, id) values (1, '20240305', '6150');

--------------------------------------------------------------------------------
-- 1. Basic COUNT cases (valid in vectorization)
--------------------------------------------------------------------------------
SELECT COUNT(pid) AS cnt_col, COUNT(*) AS cnt_star, COUNT(20240305) AS cnt_const, COUNT(DISTINCT pid) as cnt_distinct, COUNT(1) AS CNT
FROM test_vector WHERE full_date=20240305;
EXPLAIN VECTORIZATION EXPRESSION
SELECT COUNT(pid) AS cnt_col, COUNT(*) AS cnt_star, COUNT(20240305) AS cnt_const,COUNT(DISTINCT pid) as cnt_distinct, COUNT(1) AS CNT
FROM test_vector WHERE full_date=20240305;

--------------------------------------------------------------------------------
-- 2. COUNT with DISTINCT column + constant (INVALID in vectorization)
--------------------------------------------------------------------------------
SELECT COUNT(DISTINCT pid, 20240305) AS CNT FROM test_vector WHERE full_date=20240305;
EXPLAIN VECTORIZATION EXPRESSION
SELECT COUNT(DISTINCT pid, 20240305) AS CNT FROM test_vector WHERE full_date=20240305;

--------------------------------------------------------------------------------
-- 3. COUNT(DISTINCT pid, full_date) (multi-col distinct → FAIL)
--------------------------------------------------------------------------------
SELECT COUNT(DISTINCT pid, full_date) AS CNT FROM test_vector WHERE full_date=20240305;
EXPLAIN VECTORIZATION EXPRESSION
SELECT COUNT(DISTINCT pid, full_date) AS CNT FROM test_vector WHERE full_date=20240305;

--------------------------------------------------------------------------------
-- 4. COUNT(DISTINCT pid, full_date, id) (multi-col distinct → FAIL)
--------------------------------------------------------------------------------
SELECT COUNT(DISTINCT pid, full_date, id) AS CNT FROM test_vector WHERE full_date=20240305;
EXPLAIN VECTORIZATION EXPRESSION
SELECT COUNT(DISTINCT pid, full_date, id) AS CNT FROM test_vector WHERE full_date=20240305;

DROP TABLE test_vector;
