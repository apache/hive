-- test for loading into tables with the correct file format
-- test for loading into partitions with the correct file format

DROP TABLE load_wrong_fileformat_T1;
CREATE TABLE load_wrong_fileformat_T1(name STRING) STORED AS SEQUENCEFILE;
LOAD DATA LOCAL INPATH '../data/files/kv1.txt' INTO TABLE load_wrong_fileformat_T1;
