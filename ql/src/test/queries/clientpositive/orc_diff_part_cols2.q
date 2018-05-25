set hive.vectorized.execution.enabled=false;

-- Create a table with one column, write to it, then add an additional column
-- This can break reads

-- SORT_QUERY_RESULTS

CREATE TABLE test_orc_n4 (key STRING)
STORED AS ORC;

INSERT OVERWRITE TABLE test_orc_n4 SELECT key FROM src LIMIT 5;

ALTER TABLE test_orc_n4 ADD COLUMNS (value STRING);

SELECT * FROM test_orc_n4;
