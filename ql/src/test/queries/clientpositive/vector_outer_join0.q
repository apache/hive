set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=true;
set hive.fetch.task.conversion=none;

CREATE TABLE orc_table_1(v1 STRING, a INT) STORED AS ORC;
CREATE TABLE orc_table_2(c INT, v2 STRING) STORED AS ORC; 

insert into table orc_table_1 values ("<null1>", null),("one", 1),("one", 1),("two", 2),("three", 3),("<null2>", null);
insert into table orc_table_2 values (0, "ZERO"),(2, "TWO"), (3, "THREE"),(null, "<NULL1>"),(4, "FOUR"),(null, "<NULL2>");

select * from orc_table_1;
select * from orc_table_2;

explain vectorization detail
select t1.v1, t1.a, t2.c, t2.v2 from orc_table_1 t1 left outer join orc_table_2 t2 on t1.a = t2.c;

-- SORT_QUERY_RESULTS

select t1.v1, t1.a, t2.c, t2.v2 from orc_table_1 t1 left outer join orc_table_2 t2 on t1.a = t2.c;

explain vectorization detail
select t1.v1, t1.a, t2.c, t2.v2 from orc_table_1 t1 right outer join orc_table_2 t2 on t1.a = t2.c;

-- SORT_QUERY_RESULTS

select t1.v1, t1.a, t2.c, t2.v2 from orc_table_1 t1 right outer join orc_table_2 t2 on t1.a = t2.c;