CREATE TABLE repro(i STRING) STORED AS ORC;
INSERT INTO repro VALUES (NULL), (NULL), (NULL);

-- Plan optimized by CBO.
EXPLAIN
SELECT * FROM (SELECT * FROM repro) a, (SELECT * FROM repro) b, (SELECT * FROM repro) c  WHERE a.i IS NOT NULL AND b.i IS NOT NULL AND b.i = a.i AND a.i = c.i;
SELECT * FROM (SELECT * FROM repro) a, (SELECT * FROM repro) b, (SELECT * FROM repro) c  WHERE a.i IS NOT NULL AND b.i IS NOT NULL AND b.i = a.i AND a.i = c.i;

-- STATS_GENERATED_VIA_STATS_TASK                     | true
-- numRows                                            | 0
ALTER TABLE repro SET TBLPROPERTIES ('numRows' = '0', 'STATS_GENERATED_VIA_STATS_TASK' = 'true');

-- shows WARNING "Invalid Stats number of null > no of tuples"
EXPLAIN
SELECT * FROM (SELECT * FROM repro) a, (SELECT * FROM repro) b, (SELECT * FROM repro) c  WHERE a.i IS NOT NULL AND b.i IS NOT NULL AND b.i = a.i AND a.i = c.i;
-- Does not show WARNING since query is being executed
SELECT * FROM (SELECT * FROM repro) a, (SELECT * FROM repro) b, (SELECT * FROM repro) c  WHERE a.i IS NOT NULL AND b.i IS NOT NULL AND b.i = a.i AND a.i = c.i;

DROP TABLE repro;
