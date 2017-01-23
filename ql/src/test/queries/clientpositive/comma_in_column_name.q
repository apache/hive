create table test (`x,y` int);

insert into test values (1),(2);

select `x,y` from test where `x,y` >=2 ;

drop table test; 

create table test (`x,y` int) stored as orc;

insert into test values (1),(2);

select `x,y` from test where `x,y` <2 ;

