set hive.split.update=false;

drop table if exists test_update;
create external table test_update (id int, value string) stored by iceberg stored as orc tblproperties ('format-version'='2');

explain
update test_update set value='anything' where id=1;
