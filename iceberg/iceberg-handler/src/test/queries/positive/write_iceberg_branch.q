-- SORT_QUERY_RESULTS
set hive.explain.user=false;
set hive.fetch.task.conversion=more;

create external table ice01(a int, b string, c int) stored by iceberg stored as orc tblproperties ('format-version'='2');
create table source01(a int, b string, c int);

insert into ice01 values (1, 'one', 50), (2, 'two', 51), (111, 'one', 55);
insert into source01 values (1, 'one', 50), (2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54), (111, 'one', 55);

-- create a branch named test1
alter table ice01 create branch test1;

-- query branch using table identifier: db.tbl.branch_branchName
explain select * from default.ice01.branch_test1;
select * from default.ice01.branch_test1;
-- query branch using time travel syntax
select * from ice01 for system_version as of 'test1';

-- insert into branch test1
explain insert into default.ice01.branch_test1 values(22, 'three', 44), (33, 'three', 66);
insert into default.ice01.branch_test1 values(22, 'three', 44), (33, 'three', 66);
select * from default.ice01.branch_test1;

-- delete from branch test1
explain delete from default.ice01.branch_test1 where a=22;
delete from default.ice01.branch_test1 where a=22;
select * from default.ice01.branch_test1;

-- update branch test1
explain update default.ice01.branch_test1 set a=33 where c=66;
update default.ice01.branch_test1 set a=33 where c=66;
select * from default.ice01.branch_test1;

-- merge into branch test1
explain
merge into default.ice01.branch_test1 as t using source01 src ON t.a = src.a
when matched and t.a > 100 THEN DELETE
when matched then update set b = 'Merged', c = t.c + 10
when not matched then insert values (src.a, src.b, src.c);

merge into default.ice01.branch_test1 as t using source01 src ON t.a = src.a
when matched and t.a > 100 THEN DELETE
when matched then update set b = 'Merged', c = t.c + 10
when not matched then insert values (src.a, src.b, src.c);

select * from default.ice01.branch_test1;

-- insert overwrite branch test1
explain insert overwrite table default.ice01.branch_test1 values (77, 'one', 88);
insert overwrite table default.ice01.branch_test1 values (77, 'one', 88);
select * from default.ice01.branch_test1;

-- query branch using non-fetch task
set hive.fetch.task.conversion=none;
explain select * from default.ice01.branch_test1;
select * from default.ice01.branch_test1;

drop table ice01;
drop table source01;