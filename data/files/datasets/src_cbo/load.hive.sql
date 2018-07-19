set hive.cbo.enable=true;

CREATE TABLE src_cbo (key STRING, value STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv1.txt" INTO TABLE src_cbo;

analyze table src_cbo compute statistics;
analyze table src_cbo compute statistics for columns;
