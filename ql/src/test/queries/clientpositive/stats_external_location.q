set hive.stats.column.autogather=true;
set hive.stats.autogather=true;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/test1;

create external table test_custom(age int, name string) stored as orc location '/tmp/test1';
insert into test_custom select 1, 'test';
desc formatted test_custom age;

drop table test_custom;
