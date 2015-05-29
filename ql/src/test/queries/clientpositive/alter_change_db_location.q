create database newDB location "/tmp/";
describe database extended newDB;
use newDB;
create table tab (name string);
alter table tab rename to newName;
