--! qt:disabled:HIVE-25712
--! qt:dataset:src
add jar ${system:maven.local.repository}/org/apache/hive/hive-contrib/${system:hive.version}/hive-contrib-${system:hive.version}.jar;
SHOW TABLES 'src';

set datanucleus.schema.autoCreateAll=true;
set hive.metastore.ds.connection.url.hook=org.apache.hadoop.hive.contrib.metastore.hooks.SampleURLHook;
-- changes to dummy derby store.. should return empty result
SHOW TABLES 'src';
set datanucleus.schema.autoCreateAll=false;
