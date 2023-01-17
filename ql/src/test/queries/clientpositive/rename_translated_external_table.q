-- Mask the totalSize value as it can have slight variability, causing test flakiness
--! qt:replace:/(\s+totalSize\s+)\S+(\s+)/$1#Masked#$2/
set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;

create database if not exists tmp;

use tmp;

create table b(s string) stored as parquet location 'file:${system:test.tmp.dir}/tmp.db/some_location';

insert into b values ("a"),("b"),("c");

select * from b;

desc formatted b;

alter table b rename to bb;

desc formatted bb;

-- new table and location should be available after rename
create table b(s string) stored as parquet location 'file:${system:test.tmp.dir}/tmp.db/some_location';

select * from b;
