-- SORT_QUERY_RESULTS
set hive.explain.user=false;
set hive.fetch.task.conversion=more;

create external table ice01(id int) stored by iceberg stored as orc tblproperties ('format-version'='2');

insert into ice01 values (1), (2), (3), (4);

select * from ice01;

-- create one branches
alter table ice01 create branch branch1;

-- insert some values in branch1
insert into default.ice01.branch_branch1 values (5), (6);
select * from default.ice01.branch_branch1;

-- create another branch
alter table ice01 create branch branch2;
-- do some inserts & deletes on this branch
insert into default.ice01.branch_branch2 values (22), (44);
delete from default.ice01.branch_branch2 where id=2;
select * from default.ice01.branch_branch2;

-- Do a replace
explain alter table ice01 replace branch branch1 with branch2;
alter table ice01 replace branch branch1 with branch2;
select * from default.ice01.branch_branch1;