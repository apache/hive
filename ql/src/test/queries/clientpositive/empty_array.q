create table dtypes3 (c5 array<int>, c13 array<array<string>>) row format delimited fields terminated by ',' stored as TEXTFILE;
load data local inpath '../../data/files/empty_array.txt' into table dtypes3;
create table dtypes4 (c5 array<int>, c13 array<array<string>>) stored as ORC;
create table dtypes5 (c5 array<int>, c13 array<array<string>>) stored as TEXTFILE;

SET hive.vectorized.execution.enabled=true;
insert into dtypes4 select * from dtypes3;
insert into dtypes5 select * from dtypes3;

select * from dtypes4;
select * from dtypes5;
