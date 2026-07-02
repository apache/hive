-- HIVE-29473: Verify UDTF operator does not inherit parent column stats.
-- The UDTF generates brand new columns; inheriting the parent's column stats
-- causes namespace collisions in LateralViewJoinStatsRule and wrong estimates.

CREATE TABLE udtf_stats_src (id STRING, f1 STRING, arr ARRAY<STRING>)
STORED AS ORC;

ALTER TABLE udtf_stats_src UPDATE STATISTICS SET('numRows'='100','rawDataSize'='10000');
ALTER TABLE udtf_stats_src UPDATE STATISTICS FOR COLUMN id SET('numDVs'='2','numNulls'='0','avgColLen'='10','maxColLen'='20');
ALTER TABLE udtf_stats_src UPDATE STATISTICS FOR COLUMN f1 SET('numDVs'='6','numNulls'='0','avgColLen'='10','maxColLen'='20');

-- Okumin's reproducer: GROUP BY with a UDTF output column (pos1) as grouping key.
-- Pre-fix: pos1 resolves to parent's _col0 stats (NDV from f1) due to namespace
-- collision in LateralViewJoinStatsRule, producing a wrong GROUP BY cardinality.
-- Post-fix: UDTF output stats are schema-aligned placeholders; no collision occurs.
EXPLAIN
SELECT id, f1, pos1, count(*)
FROM (SELECT id, f1 FROM udtf_stats_src GROUP BY id, f1) sub
LATERAL VIEW posexplode(array(f1, f1)) t1 AS pos1, val1
GROUP BY id, f1, pos1;
