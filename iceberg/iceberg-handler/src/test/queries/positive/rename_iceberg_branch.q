-- SORT_QUERY_RESULTS
set hive.explain.user=false;
set hive.fetch.task.conversion=more;

create external table ice01(id int) stored by iceberg stored as orc tblproperties ('format-version'='2');

insert into ice01 values (1), (2), (3), (4);

select * from ice01;

-- create a branch named soruce
alter table ice01 create branch source;
select * from default.ice01.branch_source;

-- insert some data to branch
insert into ice01 values (5), (6);
select * from default.ice01.branch_source;

-- rename the branch
explain alter table ice01 rename branch source to target;
alter table ice01 rename branch source to target;

select name,type from default.ice01.refs;

-- read from the renamed branch
select * from default.ice01.branch_target;

