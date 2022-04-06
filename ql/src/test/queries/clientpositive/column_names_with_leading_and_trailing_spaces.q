set hive.mapred.mode=nonstrict;
set hive.stats.column.autogather=true;

create table space (` left` string, ` middle ` string, `right ` string);

desc formatted space;

desc formatted space ` left`;

insert into space values ("1", "2", "3");

desc formatted space ` left`;

select * from space;

insert into space (` middle `) values("2");

select * from space order by ` left`;

create table space2 (` left2` string, ` middle2 ` string);

select * from space join space2 on (` left` == ` left2`);
