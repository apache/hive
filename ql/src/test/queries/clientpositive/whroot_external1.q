--! qt:dataset:src

dfs -rmr -f hdfs:///tmp/whroot_ext;
dfs -mkdir -p hdfs:///tmp/whroot_ext;

set hive.metastore.warehouse.external.dir=hdfs:///tmp/whroot_ext;

create table wre1_managed1 (c1 string, c2 string);
show create table wre1_managed1;

insert into table wre1_managed1 select * from src where key = '0';
select count(*) from wre1_managed1;


-- external table with default location
create external table wre1_ext1 (c1 string, c2 string);
show create table wre1_ext1;

insert into table wre1_ext1 select * from src where key < 5;
select count(*) from wre1_ext1;

insert overwrite table wre1_ext1 select * from src where key < 10;
select count(*) from wre1_ext1;

load data local inpath '../../data/files/kv1.txt' overwrite into table wre1_ext1;
select count(*) from wre1_ext1;

-- external table with specified location should still work
dfs -rmr -f hdfs:///tmp/wre1_ext2;
dfs -mkdir -p hdfs:///tmp/wre1_ext2;
create external table wre1_ext2 (c1 string, c2 string) location 'hdfs:///tmp/wre1_ext2';
show create table wre1_ext2;

insert into table wre1_ext2 select * from src where key < 5;
select count(*) from wre1_ext2;

insert overwrite table wre1_ext2 select * from src where key < 10;
select count(*) from wre1_ext2;

load data local inpath '../../data/files/kv1.txt' overwrite into table wre1_ext2;
select count(*) from wre1_ext2;

-- Try with non-default db
create database wre1_db;

-- external table with default location
create external table wre1_db.wre1_ext3 (c1 string, c2 string);
show create table wre1_db.wre1_ext3;

insert into table wre1_db.wre1_ext3 select * from src where key < 5;
select count(*) from wre1_db.wre1_ext3;

insert overwrite table wre1_db.wre1_ext3 select * from src where key < 10;
select count(*) from wre1_db.wre1_ext3;

load data local inpath '../../data/files/kv1.txt' overwrite into table wre1_db.wre1_ext3;
select count(*) from wre1_db.wre1_ext3;

-- external table with specified location should still work
dfs -rmr -f hdfs:///tmp/wre1_ext4;
dfs -mkdir -p hdfs:///tmp/wre1_ext4;
create external table wre1_db.wre1_ext4 (c1 string, c2 string) location 'hdfs:///tmp/wre1_ext4';
show create table wre1_db.wre1_ext4;

insert into table wre1_db.wre1_ext4 select * from src where key < 5;
select count(*) from wre1_db.wre1_ext4;

insert overwrite table wre1_db.wre1_ext4 select * from src where key < 10;
select count(*) from wre1_db.wre1_ext4;

load data local inpath '../../data/files/kv1.txt' overwrite into table wre1_db.wre1_ext4;
select count(*) from wre1_db.wre1_ext4;

-- create table like
create external table wre1_ext5 like wre1_ext2;
show create table wre1_ext5;

insert into table wre1_ext5 select * from src where key < 5;
select count(*) from wre1_ext5;

insert overwrite table wre1_ext5 select * from src where key < 10;
select count(*) from wre1_ext5;

load data local inpath '../../data/files/kv1.txt' overwrite into table wre1_ext5;
select count(*) from wre1_ext5;


create external table wre1_db.wre1_ext6 like wre1_ext2;
show create table wre1_db.wre1_ext6;

insert into table wre1_db.wre1_ext6 select * from src where key < 5;
select count(*) from wre1_db.wre1_ext6;

insert overwrite table wre1_db.wre1_ext6 select * from src where key < 10;
select count(*) from wre1_db.wre1_ext6;

load data local inpath '../../data/files/kv1.txt' overwrite into table wre1_db.wre1_ext6;
select count(*) from wre1_db.wre1_ext6;

drop table wre1_managed1;
drop table wre1_ext1;
drop table wre1_ext2;
drop database wre1_db cascade;

dfs -rmr -f hdfs:///tmp/wre1_ext2;
dfs -rmr -f hdfs:///tmp/wre1_ext4;
dfs -rmr -f hdfs:///tmp/whroot_ext;

dfs -rmr -f  hdfs:///tmp/test_dec_space;
dfs -mkdir -p  hdfs:///tmp/test_dec_space;
dfs -copyFromLocal ../../data/files/test_dec_space.csv hdfs:///tmp/test_dec_space/;
create external table test_dec_space (id int, value decimal) ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ',' location 'hdfs:///tmp/test_dec_space';
select * from test_dec_space;

create table tbl (fld int);
dfs -mkdir -p  hdfs:///tmp/test_load_aux_jar;
dfs -copyFromLocal  ${system:hive.root}/data/files/identity_udf.jar  hdfs:///tmp/test_load_aux_jar/;

-- both hive.aux.jars.path and hive.reloadable.aux.jars.path pointing to the same jar.
SET hive.aux.jars.path=hdfs:///tmp/test_load_aux_jar/identity_udf.jar;
SET hive.reloadable.aux.jars.path=hdfs:///tmp/test_load_aux_jar/;

-- reload will load the identity_udf.jar from tmp/test_load_aux_jar
RELOAD;

insert into tbl values(1);
select * from tbl;

dfs -rmr -f hdfs:///tmp/test_load_aux_jar/