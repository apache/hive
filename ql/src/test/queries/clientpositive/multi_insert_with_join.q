set hive.auto.convert.join=false;

drop table if exists status_updates;
drop table if exists profiles;
drop table if exists school_summary;
drop table if exists gender_summary;

create table status_updates(userid int,status string,ds string);
create table profiles(userid int,school string,gender int);
create table school_summary(school string,cnt int) partitioned by (ds string);
create table gender_summary(gender int, cnt int) partitioned by (ds string);

insert into status_updates values (1, "status_1", "2009-03-20");
insert into profiles values (1, "school_1", 0);

FROM (SELECT a.status, b.school, b.gender
FROM status_updates a JOIN profiles b
ON (a.userid = b.userid and
a.ds='2009-03-20' )
) subq1
INSERT OVERWRITE TABLE gender_summary
PARTITION(ds='2009-03-20')
SELECT subq1.gender, COUNT(1) GROUP BY subq1.gender
INSERT OVERWRITE TABLE school_summary
PARTITION(ds='2009-03-20')
SELECT subq1.school, COUNT(1) GROUP BY subq1.school;

select * from school_summary;
select * from gender_summary;