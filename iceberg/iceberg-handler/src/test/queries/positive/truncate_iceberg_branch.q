-- SORT_QUERY_RESULTS
set hive.explain.user=false;
set hive.fetch.task.conversion=more;

create external table ice01(id int) stored by iceberg stored as orc tblproperties ('format-version'='2');

insert into ice01 values (1), (2), (3), (4);

select * from ice01;

-- create a branch named branch1
alter table ice01 create branch branch1;
select * from default.ice01.branch_branch1;

-- insert some data to branch
insert into ice01 values (5), (6);
select * from default.ice01.branch_branch1;

-- truncate the branch
truncate table default.ice01.branch_branch1;
select * from default.ice01.branch_branch1;

-- create a partioned iceberg table
create external table ice02(id int) partitioned by (name string) stored by iceberg stored as orc tblproperties ('format-version'='2');
insert into ice02 values (1, 'A'), (2, 'B'), (3, 'A'), (4, 'B');

select * from ice02;

-- create a branch named branch1
alter table ice02 create branch branch1;

-- insert some data to branch
insert into default.ice02.branch_branch1 values (5, 'A'), (6, 'C');
select * from default.ice02.branch_branch1;

-- truncate partition A
truncate table default.ice02.branch_branch1 partition (name='A');

select * from default.ice02.branch_branch1;

-- check original table is intact.
select * from ice02;

-- partition evolution
alter table ice02 set partition spec (id);

truncate table default.ice02.branch_branch1 partition (name='C');
select * from default.ice02.branch_branch1;