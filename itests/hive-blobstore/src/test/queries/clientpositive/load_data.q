-- Test LOAD DATA command

DROP TABLE load_data_table;
CREATE TABLE load_data_table(
    a INT,
    b STRING,
    value DOUBLE)
PARTITIONED BY (
    dateint STRING,
    hour STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
COLLECTION ITEMS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
LOCATION '${hiveconf:test.blobstore.path.unique}/load_data/load_data_table';

LOAD DATA LOCAL INPATH '../../data/files/3col_data.txt'
INTO TABLE load_data_table PARTITION (dateint="aaa", hour="bbb");

SELECT * FROM load_data_table LIMIT 2;
