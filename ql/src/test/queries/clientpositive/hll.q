--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.vectorized.execution.enabled=false;

create table n(key int);

insert overwrite table n select null from src;

explain analyze table n compute statistics for columns;

analyze table n compute statistics for columns;

desc formatted n key;


create table i(key int);

insert overwrite table i select key from src;

explain analyze table i compute statistics for columns;

analyze table i compute statistics for columns;

desc formatted i key;

drop table i;

create table i(key double);

insert overwrite table i select key from src;

analyze table i compute statistics for columns;

desc formatted i key;

drop table i;

create table i(key decimal);

insert overwrite table i select key from src;

analyze table i compute statistics for columns;

desc formatted i key;

drop table i;

create table i(key date);

insert into i values ('2012-08-17');
insert into i values ('2012-08-17');
insert into i values ('2013-08-17');
insert into i values ('2012-03-17');
insert into i values ('2012-05-17');

analyze table i compute statistics for columns;

desc formatted i key;

