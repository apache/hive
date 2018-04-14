CREATE TABLE src_thrift
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer'
WITH SERDEPROPERTIES (
  'serialization.class' = 'org.apache.hadoop.hive.serde2.thrift.test.Complex',
  'serialization.format' = 'org.apache.thrift.protocol.TBinaryProtocol')
STORED AS SEQUENCEFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/complex.seq" INTO TABLE src_thrift;

ANALYZE TABLE src_thrift COMPUTE STATISTICS;
