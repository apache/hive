create external table empty_external_table(age int, name string, id int) stored as orc;
create external table t6 like empty_external_table;
create external table t5 like empty_external_table;

-- this below is not supposed to put an empty file into the external table (instead only clear its contents)
insert overwrite table empty_external_table select a.* from t5 a full outer join t6 b on a.id=b.id and a.name=b.name and a.age=b.age;
select count(*) from empty_external_table;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/empty_external_table/;

drop table t5;
drop table t6;
drop table empty_external_table;