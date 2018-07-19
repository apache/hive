--! qt:dataset:src1
-- SORT_QUERY_RESULTS

CREATE TABLE src1_rot13_iof(key STRING, value STRING)
  STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.udf.Rot13InputFormat'
            OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.udf.Rot13OutputFormat';
DESCRIBE EXTENDED src1_rot13_iof;
SELECT * FROM src1;
INSERT OVERWRITE TABLE src1_rot13_iof SELECT * FROM src1;
SELECT * FROM src1_rot13_iof;
