-- SORT_QUERY_RESULTS
set hive.explain.user=false;
set hive.fetch.task.conversion=more;

create external table ice01(a int, b string, c int) stored by iceberg stored as orc tblproperties ('format-version'='2');

insert into ice01 values (1, 'one', 50), (2, 'two', 51), (111, 'one', 55);

select * from ice01;

-- create a branch named test1
alter table ice01 create branch test1;

select * from default.ice01.branch_test1;

-- insert into main branch
insert into ice01 values (10, 'ten', 53), (11, 'eleven', 52), (12, 'twelve', 56);
select * from ice01;

-- insert into target branch
insert into default.ice01.branch_test1 values(15, 'five', 89);
select * from ice01;

-- fastforward the branch
alter table ice01 execute fast-forward 'test1' 'main';