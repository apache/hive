drop table if exists validate_load_data;
CREATE TABLE validate_load_data(key int, value string) partitioned by (hr int) STORED AS TEXTFILE;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
LOAD DATA INPATH 'hdfs:///validateload/filedoesnt.txt' INTO TABLE validate_load_data;
SELECT * FROM validate_load_data;
DROP TABLE validate_load_data;
``````