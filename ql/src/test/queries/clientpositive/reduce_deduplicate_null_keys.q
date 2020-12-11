-- HIVE-24104: NPE due to null key columns in ReduceSink after deduplication
-- The query in this test case is not very meaningful but corresponds to a reduced and anonymized version of a query
-- used in production.
CREATE TABLE TA(id int);
INSERT INTO TA VALUES(10);

-- The explain does not fail with NPE but the problem is already present in the plan. The reduce deduplication creates
-- ReduceSink operators with nulls in the key columns something that in this case leads to NPE at runtime.
EXPLAIN
WITH
TC AS
(SELECT
   TB.i A,
   TB.i+1 B
FROM TA
LATERAL VIEW POSEXPLODE(ARRAY('a','b')) TB as i, x
ORDER BY A),
TD AS
(SELECT
    CASE WHEN A = B THEN 1 ELSE 2 END C
FROM TC)
SELECT C
FROM TD
ORDER BY C;
-- Execution fails before HIVE-24104
WITH
TC AS
(SELECT
     TB.i A,
     TB.i+1 B
FROM TA
LATERAL VIEW POSEXPLODE(ARRAY('a','b')) TB as i, x
ORDER BY A),
TD AS
(SELECT
     CASE WHEN A = B THEN 1 ELSE 2 END C
FROM TC)
SELECT C
FROM TD
ORDER BY C;
