-- SORT_QUERY_RESULTS
set hive.explain.user=false;

create table iceTbl (id int, name string) Stored by Iceberg;

-- creating tag requires table to have current snapshot. here insert some values to generate current snapshot
insert into iceTbl values(1, 'jack');

-- create tag with default values based on the current snapshotId
explain alter table iceTbl create tag test_tag_1;
alter table iceTbl create tag test_tag_1;
select name, max_reference_age_in_ms from default.iceTbl.refs where type='TAG';

-- create a tag which could be retained 5 days based on the current snapshotId
insert into iceTbl values(2, 'bob');
explain alter table iceTbl create tag test_tag_2 retain 5 days;
alter table iceTbl create tag test_tag_2 retain 5 days;
select name, max_reference_age_in_ms from default.iceTbl.refs where type='TAG';

