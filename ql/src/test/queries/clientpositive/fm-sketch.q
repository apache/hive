--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.stats.ndv.algo=fm;

create table n_n0(key int);

insert overwrite table n_n0 select null from src;

explain analyze table n_n0 compute statistics for columns;

analyze table n_n0 compute statistics for columns;

desc formatted n_n0 key;


create table i_n1(key int);

insert overwrite table i_n1 select key from src;

explain analyze table i_n1 compute statistics for columns;

analyze table i_n1 compute statistics for columns;

desc formatted i_n1 key;

drop table i_n1;

create table i_n1(key double);

insert overwrite table i_n1 select key from src;

analyze table i_n1 compute statistics for columns;

desc formatted i_n1 key;

drop table i_n1;

create table i_n1(key decimal);

insert overwrite table i_n1 select key from src;

analyze table i_n1 compute statistics for columns;

desc formatted i_n1 key;

drop table i_n1;

create table i_n1(key date);

insert into i_n1 values ('2012-08-17');
insert into i_n1 values ('2012-08-17');
insert into i_n1 values ('2013-08-17');
insert into i_n1 values ('2012-03-17');
insert into i_n1 values ('2012-05-17');

analyze table i_n1 compute statistics for columns;

desc formatted i_n1 key;

