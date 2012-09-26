

create table insert1(key int, value string) stored as textfile;
create table insert2(key int, value string) stored as textfile;
insert overwrite table insert1 select a.key, a.value from insert2 a WHERE (a.key=-1);

explain insert into table insert1 select a.key, a.value from insert2 a WHERE (a.key=-1);
explain insert into table INSERT1 select a.key, a.value from insert2 a WHERE (a.key=-1);

-- HIVE-3465
create database x;
create table x.insert1(key int, value string) stored as textfile;

explain insert into table x.INSERT1 select a.key, a.value from insert2 a WHERE (a.key=-1);

explain insert into table default.INSERT1 select a.key, a.value from insert2 a WHERE (a.key=-1);

explain
from insert2
insert into table insert1 select * where key < 10
insert overwrite table x.insert1 select * where key > 10 and key < 20;
