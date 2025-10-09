-- SORT_QUERY_RESULTS
set hive.explain.user=false;
set hive.fetch.task.conversion=more;

create external table ice01(id int) stored by iceberg stored as orc tblproperties ('format-version'='2');

insert into ice01 values (1), (2), (3), (4);

select * from ice01;

-- create a branch named test1
alter table ice01 create branch test1;
select * from default.ice01.branch_test1;

-- create a branch named test01
alter table ice01 create branch test01;

-- insert into test1 branch
insert into default.ice01.branch_test1 values (11), (21), (31), (41);
select * from default.ice01.branch_test1;

explain alter table ice01 execute fast-forward 'test1';
alter table ice01 execute fast-forward 'test1';
select * from ice01;

-- fast-forward the test01 branch to test1
explain alter table ice01 execute fast-forward 'test01' 'test1';
alter table ice01 execute fast-forward 'test01' 'test1';
select * from default.ice01.branch_test01;

-- create another branch test2
alter table ice01 create branch test2;

-- insert values to test2 branch
insert into default.ice01.branch_test2 values (12), (22), (32), (42);

-- fast-forward the main branch
alter table ice01 execute fast-forward 'main' 'test2';
select * from ice01;