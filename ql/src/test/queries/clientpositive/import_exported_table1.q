--! qt:replace:/^(?!LOCATION|.*HOOK).*(external|managed).test_b/### TABLE DIRECTORY ###/
set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;


dfs ${system:test.dfs.mkdir} /tmp/test_import_exported_table1/;
create database import_export location '/tmp/test_import_exported_table1/external' managedlocation '/tmp/test_import_exported_table1/managed';
use import_export;

create external table test_a (val string) partitioned by (pt string);
insert into test_a partition (pt ='1111') values ("asfd");
export table test_a partition (pt='1111') to '/tmp/test_a';
import table test_b from '/tmp/test_a';
show create table test_b;

dfs -ls /tmp/test_import_exported_table1/external/test_b/pt=1111;
select "============list managed directory===================";
dfs -ls -R /tmp/test_import_exported_table1/managed;
dfs -rmr /tmp/test_import_exported_table1;

drop database import_export cascade;
