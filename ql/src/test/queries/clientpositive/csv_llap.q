CREATE EXTERNAL TABLE csv_llap_test (ts int, id string, b1 boolean, b2 boolean, b3 boolean, b4 boolean)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

LOAD DATA LOCAL INPATH '../../data/files/small_csv.csv' INTO TABLE csv_llap_test;
--location '../../data/files/small_csv.csv';

SELECT MIN(ts) FROM csv_llap_test;

set hive.llap.io.cache.only=true;
--an exception would be thrown from here on for cache miss

SELECT MIN(ts) FROM csv_llap_test;
