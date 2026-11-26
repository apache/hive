-- HIVE-30037: Review reproduction: SELECT positions must survive insert target-column rearrangement.

create table pair_src (a int, b int);
insert into pair_src values (2,10), (3,20), (1,30);
create table pair_pos (a int, b int);
create table pair_named (a int, b int);

insert into pair_pos (b,a)
select a,b from pair_src tablesample (5 rows) order by 1 desc limit 1;
select 'position', a,b from pair_pos;

insert into pair_named (b,a)
select a,b from (select a,b from pair_src tablesample (5 rows) order by a desc limit 1) q;
select 'named', a,b from pair_named;

-- A missing target column must not shift the SELECT position.
create table pair_missing (a int, b int);
insert into pair_missing (b)
select b from pair_src tablesample (5 rows) order by 1 desc limit 1;
select 'missing', a,b from pair_missing;

truncate table pair_pos;
explain insert into pair_pos (b,a)
select a,b from pair_src tablesample (5 rows) sort by 1 desc limit 1;
insert into pair_pos (b,a)
select a,b from pair_src tablesample (5 rows) sort by 1 desc limit 1;
select 'sort', a,b from pair_pos;
truncate table pair_pos;
insert into pair_pos (b,a)
select a,b from pair_src tablesample (5 rows) distribute by 2 sort by 1 desc limit 1;
select 'distribute', a,b from pair_pos;
truncate table pair_pos;
insert into pair_pos (b,a)
select a,b from pair_src tablesample (5 rows) cluster by 1;
select 'cluster', a,b from pair_pos order by b;

create table pair_multi_a (a int, b int);
create table pair_multi_b (a int, b int);
from pair_src tablesample (5 rows)
insert into pair_multi_a (b,a) select a,b order by 1 desc limit 1
insert into pair_multi_b (a,b) select a,b order by 1 desc limit 1;
select 'multi_a', a,b from pair_multi_a;
select 'multi_b', a,b from pair_multi_b;

create table pair_defaults (a int default 9, b int);
insert into pair_defaults (b)
select b from pair_src tablesample (5 rows) order by 1 desc limit 1;
select 'default', a,b from pair_defaults;

create table pair_partition (a int, b int) partitioned by (p int);
insert into table pair_partition partition (p) (p,a)
select a,b from pair_src tablesample (5 rows) order by 1 desc limit 1;
select 'partition', a,b,p from pair_partition;
