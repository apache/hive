set hive.stats.fetch.column.stats=true;

create table t (col string);
insert into t values ('x');

explain
select array("b", "d", "c", "a") FROM t;

explain
select array("b", "d", "c", col) FROM t;

explain
select sort_array(array("b", "d", "c", "a")),array("1","2") FROM t;

explain
select sort_array(array("b", "d", "c", col)),array("1","2") FROM t;
