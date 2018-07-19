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
