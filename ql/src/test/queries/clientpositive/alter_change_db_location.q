create database newDB location "/tmp/";
describe database extended newDB;
use newDB;
create table tab_n13 (name string);
alter table tab_n13 rename to newName;
