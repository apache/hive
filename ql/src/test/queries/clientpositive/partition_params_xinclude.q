create table part_params_xin (customer int) partitioned by (dt string);
insert into part_params_xin partition(dt='2001-01-01') values(1);
insert into part_params_xin partition(dt='2001-01-03') values(3);

set hive.optimize.metadata.query.cache.enabled=false;

create table params(key string, value string);
insert into table params values('key1', 'value1'), ('akey1', 'avalue1'), ('akey10', 'avalue10'), ('excludekey1', 'value1'),('excludekey2', 'value1');

add jar ${system:maven.local.repository}/org/apache/hive/hive-it-qfile/${system:hive.version}/hive-it-qfile-${system:hive.version}.jar;

create temporary function alter_partition_params as 'org.apache.hadoop.hive.udf.example.AlterPartitionParamsExample';

select alter_partition_params('part_params_xin', 'dt=2001-01-01', key, value) from params;

explain extended select * from part_params_xin where nvl(dt='2001-01-01' and customer=1, false);

set metastore.partitions.parameters.include.pattern=%k_y_;
set metastore.partitions.parameters.exclude.pattern=%exclu%;
explain extended select * from part_params_xin where nvl(dt='2001-01-01' and customer=1, false);

set metastore.partitions.parameters.exclude.pattern=;
explain extended select * from part_params_xin where nvl(dt='2001-01-01' and customer=1, false);
