--! qt:dataset:src
FROM (
  FROM src
   MAP value, key
 USING 'java -cp ${system:maven.local.repository}/org/apache/hive/hive-contrib/${system:hive.version}/hive-contrib-${system:hive.version}.jar org.apache.hadoop.hive.contrib.mr.example.IdentityMapper'
    AS k, v
 CLUSTER BY k) map_output
  REDUCE k, v
   USING 'java -cp ${system:maven.local.repository}/org/apache/hive/hive-contrib/${system:hive.version}/hive-contrib-${system:hive.version}.jar org.apache.hadoop.hive.contrib.mr.example.WordCountReduce'
   AS k, v
;