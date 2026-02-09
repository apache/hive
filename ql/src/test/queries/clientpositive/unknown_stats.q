CREATE TABLE t (flag BOOLEAN);
ALTER TABLE t UPDATE STATISTICS SET('numRows'='1000');
ALTER TABLE t UPDATE STATISTICS for column flag SET ('numNulls'='-1','numTrues'='-1','numFalses'='-1');

EXPLAIN EXTENDED SELECT COUNT(*) FROM t WHERE flag;
EXPLAIN EXTENDED SELECT COUNT(*) FROM t WHERE NOT flag;
EXPLAIN EXTENDED SELECT COUNT(*) FROM t WHERE flag IS NULL;

ALTER TABLE t UPDATE STATISTICS for column flag SET ('numNulls'='0','numTrues'='-1','numFalses'='-1');

EXPLAIN EXTENDED SELECT COUNT(*) FROM t WHERE flag;
EXPLAIN EXTENDED SELECT COUNT(*) FROM t WHERE NOT flag;

ALTER TABLE t UPDATE STATISTICS for column flag SET ('numNulls'='100','numTrues'='-1','numFalses'='-1');

EXPLAIN EXTENDED SELECT COUNT(*) FROM t WHERE flag;
EXPLAIN EXTENDED SELECT COUNT(*) FROM t WHERE NOT flag;

ALTER TABLE t UPDATE STATISTICS for column flag SET ('numNulls'='-1','numTrues'='500','numFalses'='-1');

EXPLAIN EXTENDED SELECT COUNT(*) FROM t WHERE NOT flag;
EXPLAIN EXTENDED SELECT COUNT(*) FROM t WHERE flag IS NULL;

ALTER TABLE t UPDATE STATISTICS for column flag SET ('numNulls'='-1','numTrues'='-1','numFalses'='300');

EXPLAIN EXTENDED SELECT COUNT(*) FROM t WHERE flag;
EXPLAIN EXTENDED SELECT COUNT(*) FROM t WHERE flag IS NULL;

-- Test boolean NDV calculation for GROUP BY
-- Case 1: All NULL column (numTrues=0, numFalses=0) -> NDV should be 0
ALTER TABLE t UPDATE STATISTICS for column flag SET ('numNulls'='1000','numTrues'='0','numFalses'='0');
EXPLAIN EXTENDED SELECT flag, COUNT(*) FROM t GROUP BY flag;

-- Case 2: Only TRUE values (numTrues>0, numFalses=0) -> NDV should be 1
ALTER TABLE t UPDATE STATISTICS for column flag SET ('numNulls'='100','numTrues'='900','numFalses'='0');
EXPLAIN EXTENDED SELECT flag, COUNT(*) FROM t GROUP BY flag;

-- Case 3: Only FALSE values (numTrues=0, numFalses>0) -> NDV should be 1
ALTER TABLE t UPDATE STATISTICS for column flag SET ('numNulls'='100','numTrues'='0','numFalses'='900');
EXPLAIN EXTENDED SELECT flag, COUNT(*) FROM t GROUP BY flag;

-- Case 4: No TRUE, unknown FALSE (numTrues=0, numFalses=-1) -> NDV should be 1
ALTER TABLE t UPDATE STATISTICS for column flag SET ('numNulls'='100','numTrues'='0','numFalses'='-1');
EXPLAIN EXTENDED SELECT flag, COUNT(*) FROM t GROUP BY flag;

-- Case 5: Unknown TRUE, no FALSE (numTrues=-1, numFalses=0) -> NDV should be 1
ALTER TABLE t UPDATE STATISTICS for column flag SET ('numNulls'='100','numTrues'='-1','numFalses'='0');
EXPLAIN EXTENDED SELECT flag, COUNT(*) FROM t GROUP BY flag;

-- Case 6: Both values present (numTrues>0, numFalses>0) -> NDV should be 2
ALTER TABLE t UPDATE STATISTICS for column flag SET ('numNulls'='100','numTrues'='450','numFalses'='450');
EXPLAIN EXTENDED SELECT flag, COUNT(*) FROM t GROUP BY flag;

CREATE TABLE t3 (id INT);
CREATE TABLE t4 (id INT, data INT);
ALTER TABLE t3 UPDATE STATISTICS SET('numRows'='10000');
ALTER TABLE t3 UPDATE STATISTICS for column id SET ('numDVs'='10000','numNulls'='0');
ALTER TABLE t4 UPDATE STATISTICS SET('numRows'='5000');
ALTER TABLE t4 UPDATE STATISTICS for column id SET ('numDVs'='5000','numNulls'='0');
ALTER TABLE t4 UPDATE STATISTICS for column data SET ('numDVs'='100','numNulls'='-1');

EXPLAIN EXTENDED SELECT * FROM (SELECT t3.id, t4.data FROM t3 LEFT OUTER JOIN t4 ON t3.id = t4.id) sub WHERE sub.data IS NULL;
