create table test_n4 (`x,y` int);

insert into test_n4 values (1),(2);

select `x,y` from test_n4 where `x,y` >=2 ;

drop table test_n4; 

create table test_n4 (`x,y` int) stored as orc;

insert into test_n4 values (1),(2);

select `x,y` from test_n4 where `x,y` <2 ;

