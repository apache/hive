--! qt:dataset:src
--! qt:dataset:part

set hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;

-- SORT_QUERY_RESULTS

CREATE TEMPORARY TABLE test_orc_n0_temp (key STRING)
PARTITIONED BY (part STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';

set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

-- Create a table with one column write to a partition, then add an additional column and write
-- to another partition
-- This can produce unexpected results with CombineHiveInputFormat

INSERT OVERWRITE TABLE test_orc_n0_temp PARTITION (part = '1') SELECT key FROM src tablesample (5 rows) ORDER BY key;

ALTER TABLE test_orc_n0_temp ADD COLUMNS (cnt INT);

INSERT OVERWRITE TABLE test_orc_n0_temp PARTITION (part = '2') SELECT key, count(*) FROM src GROUP BY key ORDER BY key LIMIT 5;

SELECT * FROM test_orc_n0_temp;
