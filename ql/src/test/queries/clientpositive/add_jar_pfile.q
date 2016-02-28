
dfs -copyFromLocal ${system:maven.local.repository}${system:file.separator}org${system:file.separator}apache${system:file.separator}hive${system:file.separator}hive-contrib${system:file.separator}${system:hive.version}${system:file.separator}hive-contrib-${system:hive.version}.jar pfile://${system:test.tmp.dir}/hive-contrib-${system:hive.version}.jar;

add jar pfile://${system:test.tmp.dir}/hive-contrib-${system:hive.version}.jar;

CREATE TEMPORARY FUNCTION example_add AS 'org.apache.hadoop.hive.contrib.udf.example.UDFExampleAdd';

DROP TEMPORARY FUNCTION example_add;
