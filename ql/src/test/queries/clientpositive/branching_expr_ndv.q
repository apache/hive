CREATE TABLE t (cond INT, c2 STRING, c100 STRING, c0 STRING);
ALTER TABLE t UPDATE STATISTICS SET('numRows'='10000','rawDataSize'='1000000');
ALTER TABLE t UPDATE STATISTICS FOR COLUMN cond SET('numDVs'='10','numNulls'='0');
ALTER TABLE t UPDATE STATISTICS FOR COLUMN c2 SET('numDVs'='2','numNulls'='0','avgColLen'='5','maxColLen'='10');
ALTER TABLE t UPDATE STATISTICS FOR COLUMN c100 SET('numDVs'='100','numNulls'='0','avgColLen'='5','maxColLen'='10');
ALTER TABLE t UPDATE STATISTICS FOR COLUMN c0 SET('numDVs'='0','numNulls'='0','avgColLen'='5','maxColLen'='10');

-- CASE WHEN: all constants distinct (NDV=3)
EXPLAIN SELECT CASE WHEN cond=1 THEN 'A' WHEN cond=2 THEN 'B' ELSE 'C' END x FROM t GROUP BY CASE WHEN cond=1 THEN 'A' WHEN cond=2 THEN 'B' ELSE 'C' END;

-- CASE WHEN: all constants with duplicate (NDV=2)
EXPLAIN SELECT CASE WHEN cond=1 THEN 'A' WHEN cond=2 THEN 'A' ELSE 'B' END x FROM t GROUP BY CASE WHEN cond=1 THEN 'A' WHEN cond=2 THEN 'A' ELSE 'B' END;

-- CASE WHEN: constants with NULL (NDV=1, NULL literal is not a ConstantObjectInspector)
EXPLAIN SELECT CASE WHEN cond=1 THEN NULL WHEN cond=2 THEN NULL ELSE 'A' END x FROM t GROUP BY CASE WHEN cond=1 THEN NULL WHEN cond=2 THEN NULL ELSE 'A' END;

-- CASE WHEN: all NULL constants (NDV=1)
EXPLAIN SELECT CASE WHEN cond=1 THEN NULL ELSE NULL END x FROM t GROUP BY CASE WHEN cond=1 THEN NULL ELSE NULL END;

-- CASE WHEN: 3 constants + column, constants dominate (NDV=max(3,2)=3)
EXPLAIN SELECT CASE WHEN cond=1 THEN 'A' WHEN cond=2 THEN 'B' WHEN cond=3 THEN 'C' ELSE c2 END x FROM t GROUP BY CASE WHEN cond=1 THEN 'A' WHEN cond=2 THEN 'B' WHEN cond=3 THEN 'C' ELSE c2 END;

-- CASE WHEN: 2 constants + column, column dominates (NDV=max(2,100)=100)
EXPLAIN SELECT CASE WHEN cond=1 THEN 'A' WHEN cond=2 THEN 'B' ELSE c100 END x FROM t GROUP BY CASE WHEN cond=1 THEN 'A' WHEN cond=2 THEN 'B' ELSE c100 END;

-- CASE WHEN: constant + unknown column (when NDV=0, Hive uses numRows/2 fallback)
EXPLAIN SELECT CASE WHEN cond=1 THEN 'A' ELSE c0 END x FROM t GROUP BY CASE WHEN cond=1 THEN 'A' ELSE c0 END;

-- CASE WHEN: all columns, no constants (NDV=max(2,100)=100)
EXPLAIN SELECT CASE WHEN cond=1 THEN c2 ELSE c100 END x FROM t GROUP BY CASE WHEN cond=1 THEN c2 ELSE c100 END;

-- CASE WHEN: no ELSE clause (NDV=1, implicit NULL ELSE is not a ConstantObjectInspector)
EXPLAIN SELECT CASE WHEN cond=1 THEN 'A' WHEN cond=2 THEN 'B' END x FROM t GROUP BY CASE WHEN cond=1 THEN 'A' WHEN cond=2 THEN 'B' END;

-- IF: both constants distinct (NDV=2)
EXPLAIN SELECT IF(cond>5, 'A', 'B') x FROM t GROUP BY IF(cond>5, 'A', 'B');

-- IF: both constants same (NDV=1)
EXPLAIN SELECT IF(cond>5, 'A', 'A') x FROM t GROUP BY IF(cond>5, 'A', 'A');

-- IF: one NULL one constant (NDV=1, NULL literal is not a ConstantObjectInspector)
EXPLAIN SELECT IF(cond>5, NULL, 'A') x FROM t GROUP BY IF(cond>5, NULL, 'A');

-- IF: both NULL (NDV=1)
EXPLAIN SELECT IF(cond>5, NULL, NULL) x FROM t GROUP BY IF(cond>5, NULL, NULL);

-- IF: constant + column (NDV=max(1,100)=100)
EXPLAIN SELECT IF(cond>5, 'A', c100) x FROM t GROUP BY IF(cond>5, 'A', c100);

-- IF: both columns (NDV=max(2,100)=100)
EXPLAIN SELECT IF(cond>5, c2, c100) x FROM t GROUP BY IF(cond>5, c2, c100);

-- IF: constant + unknown column (when NDV=0, Hive uses numRows/2 fallback)
EXPLAIN SELECT IF(cond>5, 'A', c0) x FROM t GROUP BY IF(cond>5, 'A', c0);

-- COALESCE: all constants (NDV=1, constant-folded to first non-null 'A')
EXPLAIN SELECT COALESCE('A', 'B', 'C') x FROM t GROUP BY COALESCE('A', 'B', 'C');

-- COALESCE: constants with duplicate (NDV=1, constant-folded to 'A')
EXPLAIN SELECT COALESCE('A', 'A', 'B') x FROM t GROUP BY COALESCE('A', 'A', 'B');

-- COALESCE: column + constants, column dominates (NDV=max(2,100)=100)
EXPLAIN SELECT COALESCE(c100, 'A', 'B') x FROM t GROUP BY COALESCE(c100, 'A', 'B');

-- COALESCE: column + constants, rewritten to IF (NDV=max(1,2)=2)
EXPLAIN SELECT COALESCE(c2, 'A', 'B', 'C') x FROM t GROUP BY COALESCE(c2, 'A', 'B', 'C');

-- COALESCE: all columns (NDV=max(2,100)=100)
EXPLAIN SELECT COALESCE(c2, c100) x FROM t GROUP BY COALESCE(c2, c100);

-- COALESCE: unknown column + constant (when NDV=0, Hive uses numRows/2 fallback)
EXPLAIN SELECT COALESCE(c0, 'A') x FROM t GROUP BY COALESCE(c0, 'A');

-- COALESCE: NULL first arg simplified away, then rewritten to IF (NDV=max(1,100)=100)
EXPLAIN SELECT COALESCE(NULL, c100, 'A') x FROM t GROUP BY COALESCE(NULL, c100, 'A');
