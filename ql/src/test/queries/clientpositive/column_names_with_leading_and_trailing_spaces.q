set hive.mapred.mode=nonstrict;

create table space (` left` string, ` middle ` string, `right ` string);

desc formatted space;

insert into space values ("1", "2", "3");

select * from space;

insert into space (` middle `) values("2");

select * from space order by ` left`;
