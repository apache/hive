--! qt:dataset:src

set hive.vectorized.execution.enabled=false;

-- SORT_QUERY_RESULTS

CREATE TABLE test_orc_n3 (key STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';

INSERT OVERWRITE TABLE test_orc_n3 SELECT '' FROM src tablesample (10 rows);

-- Test reading a column which is just empty strings

SELECT * FROM test_orc_n3; 

INSERT OVERWRITE TABLE test_orc_n3 SELECT IF (key % 3 = 0, key, '') FROM src tablesample (10 rows);

-- Test reading a column which has some empty strings

SELECT * FROM test_orc_n3;
