-- HIVE-29556: GROUP BY of a column with unknown NDV (NDV=0 sentinel + numNulls>0)
-- must not collapse to "1 row" estimate. Both probes feed a join sized so that
-- master's bogus 1-row estimate triggers Map Join while the heuristic fallback
-- forces Merge Join.

SET hive.auto.convert.join=true;

CREATE TABLE big (k BIGINT);
CREATE TABLE small (id BIGINT, name STRING);

ALTER TABLE big UPDATE STATISTICS SET('numRows'='100000000');
ALTER TABLE big UPDATE STATISTICS for column k SET ('numDVs'='0','numNulls'='100000000');

ALTER TABLE small UPDATE STATISTICS SET('numRows'='1000000');
ALTER TABLE small UPDATE STATISTICS for column id   SET ('numDVs'='1000000','numNulls'='0');
ALTER TABLE small UPDATE STATISTICS for column name SET ('numDVs'='1000000','numNulls'='0','avgColLen'='100','maxColLen'='100');

-- U1: direct column with unknown NDV.
EXPLAIN
SELECT s.name, g.cnt
FROM (SELECT k, COUNT(*) AS cnt FROM big GROUP BY k) g
JOIN small s ON g.cnt = s.id;

-- U2: PessimisticStatCombiner output for CASE WHEN with one NULL branch.
EXPLAIN
SELECT s.name, g.cnt
FROM (SELECT x, COUNT(*) AS cnt
      FROM (SELECT CASE WHEN k > 0 THEN k ELSE cast(NULL AS BIGINT) END AS x FROM big) t
      GROUP BY x) g
JOIN small s ON g.cnt = s.id;
