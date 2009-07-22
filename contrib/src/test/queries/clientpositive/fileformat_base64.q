add jar ../build/contrib/hive_contrib.jar;

DROP TABLE base64_test;

EXPLAIN
CREATE TABLE base64_test(key INT, value STRING) STORED AS
  INPUTFORMAT 'org.apache.hadoop.hive.contrib.fileformat.base64.Base64TextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.contrib.fileformat.base64.Base64TextOutputFormat';

CREATE TABLE base64_test(key INT, value STRING) STORED AS
  INPUTFORMAT 'org.apache.hadoop.hive.contrib.fileformat.base64.Base64TextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.contrib.fileformat.base64.Base64TextOutputFormat';

DESCRIBE EXTENDED base64_test;

FROM src
INSERT OVERWRITE TABLE base64_test
SELECT key, value WHERE key < 10;

SELECT * FROM base64_test;


set base64.text.input.format.signature=TFT;
set base64.text.output.format.signature=TFT;

-- Base64TextInput/OutputFormat supports signature (a prefix to check the validity of
-- the data). These queries test that prefix capabilities.

FROM src
INSERT OVERWRITE TABLE base64_test
SELECT key, value WHERE key < 10;

SELECT * FROM base64_test;


DROP TABLE base64_test;
