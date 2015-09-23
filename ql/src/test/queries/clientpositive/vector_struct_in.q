set hive.cbo.enable=false;
set hive.tez.dynamic.partition.pruning=false;
set hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=true;

-- SORT_QUERY_RESULTS

-- 2 Strings
create table test_1 (`id` string, `lineid` string) stored as orc;

insert into table test_1 values ('one','1'), ('seven','1');

explain
select * from test_1 where struct(`id`, `lineid`)
IN (
struct('two','3'),
struct('three','1'),
struct('one','1'),
struct('five','2'),
struct('six','1'),
struct('eight','1'),
struct('seven','1'),
struct('nine','1'),
struct('ten','1')
);

select * from test_1 where struct(`id`, `lineid`)
IN (
struct('two','3'),
struct('three','1'),
struct('one','1'),
struct('five','2'),
struct('six','1'),
struct('eight','1'),
struct('seven','1'),
struct('nine','1'),
struct('ten','1')
);

explain
select `id`, `lineid`, struct(`id`, `lineid`)
IN (
struct('two','3'),
struct('three','1'),
struct('one','1'),
struct('five','2'),
struct('six','1'),
struct('eight','1'),
struct('seven','1'),
struct('nine','1'),
struct('ten','1')
) as b from test_1 ;

select `id`, `lineid`, struct(`id`, `lineid`)
IN (
struct('two','3'),
struct('three','1'),
struct('one','1'),
struct('five','2'),
struct('six','1'),
struct('eight','1'),
struct('seven','1'),
struct('nine','1'),
struct('ten','1')
) as b from test_1 ;


-- 2 Integers
create table test_2 (`id` int, `lineid` int) stored as orc;

insert into table test_2 values (1,1), (7,1);

explain
select * from test_2 where struct(`id`, `lineid`)
IN (
struct(2,3),
struct(3,1),
struct(1,1),
struct(5,2),
struct(6,1),
struct(8,1),
struct(7,1),
struct(9,1),
struct(10,1)
);

select * from test_2 where struct(`id`, `lineid`)
IN (
struct(2,3),
struct(3,1),
struct(1,1),
struct(5,2),
struct(6,1),
struct(8,1),
struct(7,1),
struct(9,1),
struct(10,1)
);

explain
select `id`, `lineid`, struct(`id`, `lineid`)
IN (
struct(2,3),
struct(3,1),
struct(1,1),
struct(5,2),
struct(6,1),
struct(8,1),
struct(7,1),
struct(9,1),
struct(10,1)
) as b from test_2;

select `id`, `lineid`, struct(`id`, `lineid`)
IN (
struct(2,3),
struct(3,1),
struct(1,1),
struct(5,2),
struct(6,1),
struct(8,1),
struct(7,1),
struct(9,1),
struct(10,1)
) as b from test_2;

-- 1 String and 1 Integer
create table test_3 (`id` string, `lineid` int) stored as orc;

insert into table test_3 values ('one',1), ('seven',1);

explain
select * from test_3 where struct(`id`, `lineid`)
IN (
struct('two',3),
struct('three',1),
struct('one',1),
struct('five',2),
struct('six',1),
struct('eight',1),
struct('seven',1),
struct('nine',1),
struct('ten',1)
);

select * from test_3 where struct(`id`, `lineid`)
IN (
struct('two',3),
struct('three',1),
struct('one',1),
struct('five',2),
struct('six',1),
struct('eight',1),
struct('seven',1),
struct('nine',1),
struct('ten',1)
);

explain
select `id`, `lineid`, struct(`id`, `lineid`)
IN (
struct('two',3),
struct('three',1),
struct('one',1),
struct('five',2),
struct('six',1),
struct('eight',1),
struct('seven',1),
struct('nine',1),
struct('ten',1)
) as b from test_3;

select `id`, `lineid`, struct(`id`, `lineid`)
IN (
struct('two',3),
struct('three',1),
struct('one',1),
struct('five',2),
struct('six',1),
struct('eight',1),
struct('seven',1),
struct('nine',1),
struct('ten',1)
) as b from test_3;

-- 1 Integer and 1 String and 1 Double
create table test_4 (`my_bigint` bigint, `my_string` string, `my_double` double) stored as orc;

insert into table test_4 values (1, "b", 1.5), (1, "a", 0.5), (2, "b", 1.5);

explain
select * from test_4 where struct(`my_bigint`, `my_string`, `my_double`)
IN (
struct(1L, "a", 1.5),
struct(1L, "b", -0.5),
struct(3L, "b", 1.5),
struct(1L, "d", 1.5),
struct(1L, "c", 1.5),
struct(1L, "b", 2.5),
struct(1L, "b", 0.5),
struct(5L, "b", 1.5),
struct(1L, "a", 0.5),
struct(3L, "b", 1.5)
);

select * from test_4 where struct(`my_bigint`, `my_string`, `my_double`)
IN (
struct(1L, "a", 1.5),
struct(1L, "b", -0.5),
struct(3L, "b", 1.5),
struct(1L, "d", 1.5),
struct(1L, "c", 1.5),
struct(1L, "b", 2.5),
struct(1L, "b", 0.5),
struct(5L, "b", 1.5),
struct(1L, "a", 0.5),
struct(3L, "b", 1.5)
);

explain
select `my_bigint`, `my_string`, `my_double`, struct(`my_bigint`, `my_string`, `my_double`)
IN (
struct(1L, "a", 1.5),
struct(1L, "b", -0.5),
struct(3L, "b", 1.5),
struct(1L, "d", 1.5),
struct(1L, "c", 1.5),
struct(1L, "b", 2.5),
struct(1L, "b", 0.5),
struct(5L, "b", 1.5),
struct(1L, "a", 0.5),
struct(3L, "b", 1.5)
) as b from test_4;

select `my_bigint`, `my_string`, `my_double`, struct(`my_bigint`, `my_string`, `my_double`)
IN (
struct(1L, "a", 1.5),
struct(1L, "b", -0.5),
struct(3L, "b", 1.5),
struct(1L, "d", 1.5),
struct(1L, "c", 1.5),
struct(1L, "b", 2.5),
struct(1L, "b", 0.5),
struct(5L, "b", 1.5),
struct(1L, "a", 0.5),
struct(3L, "b", 1.5)
) as b from test_4;