create table test_n4 (`x,y` int);

insert into test_n4 values (1),(2);

explain vectorization
select `x,y` from test_n4 where `x,y` >=2;
select `x,y` from test_n4 where `x,y` >=2;

SET hive.fetch.task.conversion=none;
explain vectorization
select `x,y` from test_n4 where `x,y` >=2;
select `x,y` from test_n4 where `x,y` >=2;

drop table test_n4;

SET hive.fetch.task.conversion=more;

create table test_n4 (`x,y` int) stored as orc;

insert into test_n4 values (1),(2);

explain vectorization
select `x,y` from test_n4 where `x,y` <2;
select `x,y` from test_n4 where `x,y` <2;

SET hive.fetch.task.conversion=none;
explain vectorization
select `x,y` from test_n4 where `x,y` <2;
select `x,y` from test_n4 where `x,y` <2;
