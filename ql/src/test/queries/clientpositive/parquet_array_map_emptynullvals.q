drop table if exists testSets;
drop table if exists testSets2;
create table testSets (
key string,
arrayValues array<string>,
mapValues map<string,string>)
stored as parquet;

insert into table testSets select 'abcd', array(), map() from src limit 1;

create table testSets2 (
key string,
arrayValues array<string>,
mapValues map<string,string>)
stored as parquet;
insert into table testSets2 select * from testSets;
select * from testSets2;
drop table testSets;
drop table testSets2;

