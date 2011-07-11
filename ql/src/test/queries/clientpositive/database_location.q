create database db1;
describe database extended db1;
use db1;
create table table_db1 (name string, value int);
describe formatted table_db1;
show tables;

create database db2 location '${hiveconf:hive.metastore.warehouse.dir}/db2';
describe database extended db2;
use db2;
create table table_db2 (name string, value int);
describe formatted table_db2;
show tables;

drop database db2 cascade;
drop database db1 cascade;