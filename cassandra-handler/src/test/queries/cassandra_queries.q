SET hive.support.concurrency=false;
add file conf/cassandra.yaml;

DROP TABLE cassandra_hive_table;

CREATE EXTERNAL TABLE
cassandra_hive_table(key int, value string)
STORED BY 'org.apache.hadoop.hive.cassandra.CassandraStorageHandler'
WITH SERDEPROPERTIES ("cassandra.columns.mapping" = ":key,value" , "cassandra.cf.name" = "Table" , "cassandra.host" = "127.0.0.1" , "cassandra.port" = "9170", "cassandra.partitioner" = "org.apache.cassandra.dht.RandomPartitioner" )
TBLPROPERTIES ("cassandra.ks.name" = "Hive", "cassandra.ks.repfactor" = "1", "cassandra.ks.strategy" = "org.apache.cassandra.locator.SimpleStrategy");

DESCRIBE cassandra_hive_table;

EXPLAIN FROM src INSERT OVERWRITE TABLE cassandra_hive_table SELECT * WHERE (key%7)=0 ORDER BY key;
FROM src INSERT OVERWRITE TABLE cassandra_hive_table SELECT * WHERE (key%7)=0 ORDER BY key;

EXPLAIN select * from cassandra_hive_table ORDER BY key;
select * from cassandra_hive_table ORDER BY key;

EXPLAIN select value from cassandra_hive_table ORDER BY VALUE;
select value from cassandra_hive_table ORDER BY VALUE;

EXPLAIN select a.key,a.value,b.value from cassandra_hive_table a JOIN cassandra_hive_table b on a.key=b.key ORDER BY a.key;
select a.key,a.value,b.value from cassandra_hive_table a JOIN cassandra_hive_table b on a.key=b.key ORDER BY a.key;