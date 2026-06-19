set hive.explain.user=false;
set hive.stats.fetch.column.stats=true;
set hive.stats.max.variable.length=10000;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask.size=10000;

create table t (key string, value string);

insert into t values ('a', 'a'), ('b', 'b'), ('c', 'c'), ('D', 'D'), ('E', 'E');

analyze table t compute statistics for columns;

explain select a.key, lower(a.value) from t a join t b on a.key = upper(b.key);
