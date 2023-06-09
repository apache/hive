create external table ice01(a int, b string, c int) stored by iceberg;

-- insert into branch test1 which does not exist
insert into default.ice01.branch_test1 values(11, 'one', 22);