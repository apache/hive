
add jar ${system:maven.local.repository}/org/apache/hive/hcatalog/hive-hcatalog-core/${system:hive.version}/hive-hcatalog-core-${system:hive.version}.jar;

CREATE TABLE t1_n156 (c1 int, c2 string, c3 timestamp)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
WITH SERDEPROPERTIES ('timestamp.formats'='yyyy-MM-dd\'T\'HH:mm:ss')
;
LOAD DATA LOCAL INPATH "../../data/files/tsformat.json" INTO TABLE t1_n156;
select a.c1, a.c2, b.c3
from t1_n156 a join t1_n156 b on a.c1 = b.c1;

drop table t1_n156;
