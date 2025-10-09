drop table if exists validate_load_data;
CREATE TABLE validate_load_data(key int, value string) partitioned by (hr int) STORED AS TEXTFILE;
LOAD DATA INPATH '/tmp/kv1.txt' INTO TABLE validate_load_data;
SELECT * FROM validate_load_data;
DROP TABLE validate_load_data;
``````