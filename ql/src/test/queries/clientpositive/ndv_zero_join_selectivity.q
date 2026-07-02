CREATE TABLE ndv_zero_t1 (id BIGINT, data STRING);
CREATE TABLE ndv_zero_t2 (id BIGINT, value STRING);

ALTER TABLE ndv_zero_t1 UPDATE STATISTICS SET('numRows'='100000000','rawDataSize'='1000000000');
ALTER TABLE ndv_zero_t1 UPDATE STATISTICS FOR COLUMN id SET('numDVs'='0','numNulls'='0');
ALTER TABLE ndv_zero_t1 UPDATE STATISTICS FOR COLUMN data SET('numDVs'='1000','numNulls'='0','avgColLen'='10','maxColLen'='50');

ALTER TABLE ndv_zero_t2 UPDATE STATISTICS SET('numRows'='100000000','rawDataSize'='1000000000');
ALTER TABLE ndv_zero_t2 UPDATE STATISTICS FOR COLUMN id SET('numDVs'='0','numNulls'='0');
ALTER TABLE ndv_zero_t2 UPDATE STATISTICS FOR COLUMN value SET('numDVs'='1000','numNulls'='0','avgColLen'='10','maxColLen'='50');

EXPLAIN
SELECT t1.id, t2.value
FROM ndv_zero_t1 t1
JOIN ndv_zero_t2 t2 ON t1.id = t2.id;
