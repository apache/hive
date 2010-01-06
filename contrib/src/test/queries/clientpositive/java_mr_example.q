FROM (
  FROM src
   MAP value, key
 USING 'java -cp ../build/contrib/hive_contrib.jar org.apache.hadoop.hive.contrib.mr.example.IdentityMapper'
    AS k, v
 CLUSTER BY k) map_output
  REDUCE k, v
   USING 'java -cp ../build/contrib/hive_contrib.jar org.apache.hadoop.hive.contrib.mr.example.WordCountReduce'
   AS k, v
;