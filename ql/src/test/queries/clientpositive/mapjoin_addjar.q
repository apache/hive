
set hive.auto.convert.join=true;
set hive.auto.convert.join.use.nonstaged=false;

ADD JAR ${system:maven.local.repository}/org/apache/hive/hive-it-test-serde/${system:hive.version}/hive-it-test-serde-${system:hive.version}.jar;

CREATE TABLE t1(KEY STRING, VALUE STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.TestSerDe' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/kv1_cb.txt' INTO TABLE t1;

select * from t1 l join t1 r on l.key =r.key;

drop table t1;
DELETE JAR ${system:maven.local.repository}/org/apache/hive/hive-it-test-serde/${system:hive.version}/hive-it-test-serde-${system:hive.version}.jar;
set hive.auto.convert.join=false;

