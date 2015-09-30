
dfs -copyFromLocal ${system:maven.local.repository}/org/apache/hive/hive-contrib/${system:hive.version}/hive-contrib-${system:hive.version}.jar pfile://${system:test.tmp.dir}/hive-contrib-${system:hive.version}.jar;

add jar pfile://${system:test.tmp.dir}/hive-contrib-${system:hive.version}.jar;

CREATE TEMPORARY FUNCTION example_add AS 'org.apache.hadoop.hive.contrib.udf.example.UDFExampleAdd';

DROP TEMPORARY FUNCTION example_add;
