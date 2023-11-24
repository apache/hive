set hive.split.update=false;

drop table if exists test_merge_target;
drop table if exists test_merge_source;
create external table test_merge_target (a int, b string, c int) stored by iceberg stored as orc tblproperties ('format-version'='2');
create external table test_merge_source (a int, b string, c int) stored by iceberg stored as orc;

explain
merge into test_merge_target as t using test_merge_source src ON t.a = src.a
when matched then update set b = 'Merged', c = t.c + 10
when not matched then insert values (src.a, src.b, src.c);
