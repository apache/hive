--SETUP----------------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE csv_llap_test (ts int, id string, b1 boolean, b2 boolean, b3 boolean, b4 boolean)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '../../data/files/csv';

CREATE EXTERNAL TABLE csv_llap_test_differentschema (ts int, id string, b1 boolean)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '../../data/files/csv';


--TEST that cache lookup results in a hit, even for OpenCSVSerde-------------------------------------------------------
SELECT MIN(ts) FROM csv_llap_test;

set hive.llap.io.cache.only=true;
--an exception would be thrown from here on for cache miss

SELECT MIN(ts) FROM csv_llap_test;

set hive.llap.io.cache.only=false;

--TEST that another table with different schema defined on the same text files works without cache problems------------
SELECT MIN(ts) FROM csv_llap_test_differentschema;
