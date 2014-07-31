add jar ${system:maven.local.repository}/org/apache/hive/hive-contrib/${system:hive.version}/hive-contrib-${system:hive.version}.jar;
SHOW TABLES 'src';

set hive.metastore.ds.connection.url.hook=org.apache.hadoop.hive.contrib.metastore.hooks.TestURLHook;
-- changes to dummy derby store.. should return empty result
SHOW TABLES 'src';
