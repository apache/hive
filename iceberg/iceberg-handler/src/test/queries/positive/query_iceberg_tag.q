-- SORT_QUERY_RESULTS
set hive.explain.user=false;
set hive.fetch.task.conversion=more;

create external table ice01(a int, b string, c int) stored by iceberg stored as orc tblproperties ('format-version'='2');
insert into ice01 values (1, 'one', 50), (2, 'two', 51), (111, 'one', 55);

-- create a tag named test1
alter table ice01 create tag test1;

-- query tag using table identifier: db.tbl.tag_tagName
explain select * from default.ice01.tag_test1;
select * from default.ice01.tag_test1;

-- query tag using non-fetch task
set hive.fetch.task.conversion=none;
explain select * from default.ice01.tag_test1;
select * from default.ice01.tag_test1;

drop table ice01;