-- Test: NDV=0 (unknown) causes incorrect join selectivity calculation
-- When column statistics have NDV=0, the join selectivity incorrectly becomes 1.0
-- This leads to cross-product cardinality estimates: rows1 * rows2
-- Bug location: HiveRelMdSelectivity.getMaxNDVFromProjections()

CREATE TABLE ndv_zero_t1 (id BIGINT, data STRING);
CREATE TABLE ndv_zero_t2 (id BIGINT, value STRING);

-- Set up large row counts but NDV=0 (unknown) for join columns
ALTER TABLE ndv_zero_t1 UPDATE STATISTICS SET('numRows'='100000000','rawDataSize'='1000000000');
ALTER TABLE ndv_zero_t1 UPDATE STATISTICS FOR COLUMN id SET('numDVs'='0','numNulls'='0');
ALTER TABLE ndv_zero_t1 UPDATE STATISTICS FOR COLUMN data SET('numDVs'='1000','numNulls'='0','avgColLen'='10','maxColLen'='50');

ALTER TABLE ndv_zero_t2 UPDATE STATISTICS SET('numRows'='100000000','rawDataSize'='1000000000');
ALTER TABLE ndv_zero_t2 UPDATE STATISTICS FOR COLUMN id SET('numDVs'='0','numNulls'='0');
ALTER TABLE ndv_zero_t2 UPDATE STATISTICS FOR COLUMN value SET('numDVs'='1000','numNulls'='0','avgColLen'='10','maxColLen'='50');

-- BUG: With NDV=0 on join columns, selectivity becomes 1.0 (cross product)
-- Expected cardinality should be reasonable (e.g., 100M if NDV=100M)
-- Actual cardinality will be 100M * 100M = 10 quadrillion (cross product)
EXPLAIN
SELECT t1.id, t2.value
FROM ndv_zero_t1 t1
JOIN ndv_zero_t2 t2 ON t1.id = t2.id;
