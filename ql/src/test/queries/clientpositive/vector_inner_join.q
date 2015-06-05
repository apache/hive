SET hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=true;

CREATE TABLE orc_table_1a(a INT) STORED AS ORC;
CREATE TABLE orc_table_2a(c INT) STORED AS ORC; 

insert into table orc_table_1a values(1),(1), (2),(3);
insert into table orc_table_2a values(0),(2), (3),(null),(4);

explain
select t1.a from orc_table_2a t2 join orc_table_1a t1 on t1.a = t2.c where t1.a > 2;

select t1.a from orc_table_2a t2 join orc_table_1a t1 on t1.a = t2.c where t1.a > 2;

explain
select t2.c from orc_table_2a t2 left semi join orc_table_1a t1 on t1.a = t2.c where t2.c > 2;

select t2.c from orc_table_2a t2 left semi join orc_table_1a t1 on t1.a = t2.c where t2.c > 2;


CREATE TABLE orc_table_1b(v1 STRING, a INT) STORED AS ORC;
CREATE TABLE orc_table_2b(c INT, v2 STRING) STORED AS ORC; 

insert into table orc_table_1b values("one", 1),("one", 1), ("two", 2),("three", 3);
insert into table orc_table_2b values(0, "ZERO"),(2, "TWO"), (3, "THREE"),(null, "<NULL>"),(4, "FOUR");

explain
select t1.v1, t1.a from orc_table_2b t2 join orc_table_1b t1 on t1.a = t2.c where t1.a > 2;

select t1.v1, t1.a from orc_table_2b t2 join orc_table_1b t1 on t1.a = t2.c where t1.a > 2;


explain
select t1.v1, t1.a, t2.c, t2.v2 from orc_table_2b t2 join orc_table_1b t1 on t1.a = t2.c where t1.a > 2;

select t1.v1, t1.a, t2.c, t2.v2 from orc_table_2b t2 join orc_table_1b t1 on t1.a = t2.c where t1.a > 2;

explain
select t1.v1, t1.a*2, t2.c*5, t2.v2 from orc_table_2b t2 join orc_table_1b t1 on t1.a = t2.c where t1.a > 2;

select t1.v1, t1.a*2, t2.c*5, t2.v2 from orc_table_2b t2 join orc_table_1b t1 on t1.a = t2.c where t1.a > 2;

explain
select t1.v1, t2.v2, t2.c from orc_table_2b t2 join orc_table_1b t1 on t1.a = t2.c where t1.a > 2;

select t1.v1, t2.v2, t2.c from orc_table_2b t2 join orc_table_1b t1 on t1.a = t2.c where t1.a > 2;

explain
select t1.a, t1.v1, t2.v2 from orc_table_2b t2 join orc_table_1b t1 on t1.a = t2.c where t1.a > 2;

select t1.a, t1.v1, t2.v2 from orc_table_2b t2 join orc_table_1b t1 on t1.a = t2.c where t1.a > 2;

explain
select t1.v1, t2.v2, t2.c from orc_table_1b t1 join orc_table_2b t2 on t1.a = t2.c where t1.a > 2;

select t1.v1, t2.v2, t2.c from orc_table_1b t1 join orc_table_2b t2 on t1.a = t2.c where t1.a > 2;

explain
select t1.a, t1.v1, t2.v2 from orc_table_1b t1 join orc_table_2b t2 on t1.a = t2.c where t1.a > 2;

select t1.a, t1.v1, t2.v2 from orc_table_1b t1 join orc_table_2b t2 on t1.a = t2.c where t1.a > 2;
