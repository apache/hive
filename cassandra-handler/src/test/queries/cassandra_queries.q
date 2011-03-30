SET hive.support.concurrency=false;
add file conf/cassandra.yaml;

CREATE EXTERNAL TABLE IF NOT EXISTS 
cassandra_keyspace1_standard1(key int, value string)
STORED BY 'org.apache.hadoop.hive.cassandra.CassandraStorageHandler'
WITH SERDEPROPERTIES ("cassandra.columns.mapping" = ":key,Standard1:value" , "cassandra.cf.name" = "Standard1" , "cassandra.host" = "127.0.0.1" , "cassandra.port" = "9170", "cassandra.cf.name" = "Standard1" , "cassandra.partitioner" = "org.apache.cassandra.dht.RandomPartitioner" )
TBLPROPERTIES ("cassandra.ks.name" = "Keyspace1");

describe cassandra_keyspace1_standard1;

EXPLAIN 
FROM src INSERT OVERWRITE TABLE cassandra_keyspace1_standard1 SELECT * WHERE (key%7)=0 ORDER BY key;

FROM src INSERT OVERWRITE TABLE cassandra_keyspace1_standard1 SELECT * WHERE (key%7)=0 ORDER BY key;

EXPLAIN
select * from cassandra_keyspace1_standard1 ORDER BY key;
select * from cassandra_keyspace1_standard1 ORDER BY key;

EXPLAIN
select value from cassandra_keyspace1_standard1 ORDER BY VALUE;

select value from cassandra_keyspace1_standard1 ORDER BY VALUE;

EXPLAIN
select a.key,a.value,b.value from cassandra_keyspace1_standard1 a JOIN cassandra_keyspace1_standard1 b on a.key=b.key ORDER BY a.key; 

select a.key,a.value,b.value from cassandra_keyspace1_standard1 a JOIN cassandra_keyspace1_standard1 b on a.key=b.key ORDER BY a.key;
