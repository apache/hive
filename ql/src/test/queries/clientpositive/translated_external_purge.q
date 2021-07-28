set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;
set metastore.metadata.transformer.location.mode=seqsuffix;

set hive.fetch.task.conversion=none;
set hive.compute.query.using.stats=false;

dfs -mkdir -p  ${system:test.tmp.dir}/etp_1;
dfs -copyFromLocal ../../data/files/kv1.txt ${system:test.tmp.dir}/etp_1/;

-- -- EXTERNAL table
create external table c_ext(c int) STORED AS textfile LOCATION '${system:test.tmp.dir}/etp_1' TBLPROPERTIES('external.table.purge'='false');

-- Maintain the purge=false property set above
desc formatted c_ext;
select count(*) from c_ext;
drop table c_ext;

-- Create external table in same location, data should still be there
create external table c_ext(c int) STORED AS textfile LOCATION '${system:test.tmp.dir}/etp_1' TBLPROPERTIES('external.table.purge'='false');
desc formatted c_ext;
select count(*) from c_ext;


-- Non-ACID table will be translated to EXTERNAL
create table c(c int) LOCATION '${system:test.tmp.dir}/etp_1' TBLPROPERTIES('transactional'='false','external.table.purge'='false');
insert into c values(1);

-- Maintain the purge=false property set above
desc formatted c;
select count(*) from c;
drop table c;

-- Create table in same location, data should still be there
create table c(c int) LOCATION '${system:test.tmp.dir}/etp_1' TBLPROPERTIES('transactional'='false','external.table.purge'='false');
desc formatted c;
select count(*) from c;
