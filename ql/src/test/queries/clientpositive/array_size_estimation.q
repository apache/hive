set hive.stats.fetch.column.stats=true;

create table t_n19 (col string);
insert into t_n19 values ('x');

explain
select array("b", "d", "c", "a") FROM t_n19;

explain
select array("b", "d", "c", col) FROM t_n19;

explain
select sort_array(array("b", "d", "c", "a")),array("1","2") FROM t_n19;

explain
select sort_array(array("b", "d", "c", col)),array("1","2") FROM t_n19;
