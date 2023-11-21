-- SORT_QUERY_RESULTS
set hive.explain.user=false;

create table iceTbl (id int, name string) Stored by Iceberg;

-- create a branch on an empty table
explain alter table iceTbl create branch test_branch_0;
alter table iceTbl create branch test_branch_0;

insert into iceTbl values(1, 'jack');

-- create s branch test_branch_1 with default values based on the current snapshotId
explain alter table iceTbl create branch test_branch_1;
alter table iceTbl create branch test_branch_1;
-- check the values, one value
select * from iceTbl for system_version as of 'test_branch_1';

-- create a branch test_branch_2 which could be retained 5 days based on the current snapshotId
insert into iceTbl values(2, 'bob');
explain alter table iceTbl create branch test_branch_2 retain 5 days;
alter table iceTbl create branch test_branch_2 retain 5 days;
-- check the values, two values
select * from iceTbl for system_version as of 'test_branch_2';

-- create a branch test_branch_3 with 5 snapshots based on the current snapshotId
insert into iceTbl values(3, 'tom');
explain alter table iceTbl create branch test_branch_3 with snapshot retention 5 snapshots;
alter table iceTbl create branch test_branch_3 with snapshot retention 5 snapshots;
-- check the values, three values
select * from iceTbl for system_version as of 'test_branch_3';

-- create a branch test_branch_4 based on the current snapshotId that has 5 snapshots, each of which is retained for 5 days
insert into iceTbl values(4, 'lisa');
explain alter table iceTbl create branch test_branch_4 with snapshot retention 5 snapshots 5 days;
alter table iceTbl create branch test_branch_4 with snapshot retention 5 snapshots 5 days;
-- check the values, four values
select * from iceTbl for system_version as of 'test_branch_4';

-- Create a branch based on an existing tag.
alter table iceTbl create tag test_tag;
explain alter table iceTbl create branch test_branch_10 for tag as of test_tag;
alter table iceTbl create branch test_branch_10 for tag as of test_tag;

-- drop a branch
explain alter table iceTbl drop branch test_branch_3;
alter table iceTbl drop branch test_branch_3;

-- drop a branch with if exists
explain alter table iceTbl drop branch if exists test_branch_4;
alter table iceTbl drop branch if exists test_branch_4;

-- drop a non-exist branch with if exists
alter table iceTbl drop branch if exists test_branch_5;
