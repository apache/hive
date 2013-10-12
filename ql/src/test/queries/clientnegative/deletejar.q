
ADD JAR ${system:maven.local.repository}/org/apache/hive/hive-internal/${system:hive.version}/hive-internal-${system:hive.version}-test-serde.jar;
DELETE JAR ${system:maven.local.repository}/org/apache/hive/hive-internal/${system:hive.version}/hive-internal-${system:hive.version}-test-serde.jar;
CREATE TABLE DELETEJAR(KEY STRING, VALUE STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.TestSerDe' STORED AS TEXTFILE;
