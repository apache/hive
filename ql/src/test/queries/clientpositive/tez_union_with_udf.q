--! qt:dataset:src
select * from (select key + key from src limit 1) a
union all
select * from (select key + key from src limit 1) b;


add jar ${system:maven.local.repository}/org/apache/hive/hive-it-test-serde/${system:hive.version}/hive-it-test-serde-${system:hive.version}.jar;

create temporary function example_add as 'org.apache.hadoop.hive.udf.example.GenericUDFExampleAdd';

-- Now try the query with the UDF
select example_add(key, key)from (select key from src limit 1) a
union all
select example_add(key, key)from (select key from src limit 1) b;
