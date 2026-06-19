--! qt:dataset:src
add jar ${system:maven.local.repository}/org/apache/hive/hive-contrib/${system:hive.version}/hive-contrib-${system:hive.version}.jar;

CREATE TEMPORARY FUNCTION udtfCount2 AS 'org.apache.hadoop.hive.contrib.udtf.example.GenericUDTFCount2';

SELECT udtfCount2(key) AS count FROM src;

SELECT * FROM src LATERAL VIEW udtfCount2(key) myTable AS myCol;

DROP TEMPORARY FUNCTION udtfCount;