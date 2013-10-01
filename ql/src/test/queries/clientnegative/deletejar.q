
ADD JAR ../build/ql/test/TestSerDe.jar;
DELETE JAR ../build/ql/test/TestSerDe.jar;
CREATE TABLE DELETEJAR(KEY STRING, VALUE STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.TestSerDe' STORED AS TEXTFILE;
