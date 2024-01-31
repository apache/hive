--load data if the file does not exist

CREATE TABLE validate_load_data(key int, value string) partitioned by (hr int) STORED AS TEXTFILE;
LOAD DATA INPATH 'hdfs:///validateload/filedoesnt.txt' INTO TABLE validate_load_data partition (hr);
SELECT * FROM validate_load_data;
DROP TABLE validate_load_data;
```