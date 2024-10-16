--! qt:replace:/^(?!LOCATION|.*HOOK).*(external|managed).test_b/### TABLE DIRECTORY ###/
set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;

dfs ${system:test.dfs.mkdir} /tmp/test_import_exported_table/;
dfs ${system:test.dfs.mkdir} /tmp/test_import_exported_table/exported_table/;
dfs ${system:test.dfs.mkdir} /tmp/test_import_exported_table/exported_table/data/;

dfs -copyFromLocal ../../data/files/exported_table/_metadata /tmp/test_import_exported_table/exported_table;
dfs -copyFromLocal ../../data/files/exported_table/data/data /tmp/test_import_exported_table/exported_table/data;

IMPORT FROM '/tmp/test_import_exported_table/exported_table';
DESCRIBE j1_41;
SELECT * from j1_41;

dfs -rmr /tmp/test_import_exported_table;

dfs ${system:test.dfs.mkdir} /tmp/test_import_exported_table/;
create database import_export location '/tmp/test_import_exported_table/external' managedlocation '/tmp/test_import_exported_table/managed';
use import_export;

create external table test_a (val string) partitioned by (pt string);
insert into test_a partition (pt ='1111') values ("asfd");
export table test_a partition (pt='1111') to '/tmp/test_a';
import table test_b from '/tmp/test_a';
show create table test_b;

dfs -ls /tmp/test_import_exported_table/external/test_b/pt=1111;
select "============list managed directory===================";
dfs -ls -R /tmp/test_import_exported_table/managed;
dfs -rmr /tmp/test_import_exported_table;

drop database import_export cascade;

