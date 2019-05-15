--! qt:dataset:src
--! qt:dataset:part
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.merge.mapfiles=false;	
set hive.merge.mapredfiles=false; 
set mapred.input.dir.recursive=true;
set hive.stats.dbclass=fs;
-- Tests truncating a column from a list bucketing table


CREATE TABLE test_tab_n0 (key STRING, value STRING) PARTITIONED BY (part STRING) STORED AS RCFILE;

ALTER TABLE test_tab_n0 SKEWED BY (key) ON ("484") STORED AS DIRECTORIES;

INSERT OVERWRITE TABLE test_tab_n0 PARTITION (part = '1') SELECT * FROM src;

describe formatted test_tab_n0 partition (part='1');
