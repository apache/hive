-- SORT_QUERY_RESULTS

create table ice01 (id int) stored by iceberg;
insert into ice01 values (1), (2), (3), (4);

--- create branch
create branch branch1 FROM ice01;

-- insert some values in branch1
insert into default.ice01.branch_branch1 values (5), (6);
select * from default.ice01.branch_branch1;

-- replace branch
create or replace branch test_branch_1 FROM ice01;
select * from default.ice01.branch_branch1;

-- create branch if not exist
create branch if not exists branch1 FROM ice01;

-- create tag
create tag tag1 FROM ice01;
select * from default.ice01.tag_tag1;

-- replace tag
delete from ice01 where id=2;
create or replace tag tag1 FROM ice01;
select * from default.ice01.tag_tag1;

-- create tag if not exists
create tag if not exists tag1 FROM ice01;

-- drop branch
drop branch branch1 FROM ice01;

--drop tag
drop tag tag1 FROM ice01;
