-- HIVE-30037: CBO declines scripts; ORDER BY positions count the TRANSFORM output columns.
-- Runs on TestMiniLlapCliDriver: hive.llap.execution.mode=only on the local driver rejects script operators.

create table pacd_tr (d int, s string);
insert into pacd_tr values (1, 'b'), (3, 'a'), (2, 'c');

select transform(d, s) using 'cat' as (x, y) from pacd_tr order by 2 desc;

-- Explicit target-column lists must not make positions refer to script inputs.
-- Compare the positional key with the corresponding named script output.
create table pacd_tr_src (a int, b int);
insert into pacd_tr_src values (2,10), (3,20), (1,30);
create table pacd_tr_pos (a int, b int);
create table pacd_tr_named (a int, b int);
insert into pacd_tr_pos (b,a)
select transform(a,b) using 'cat' as (x int,y int) from pacd_tr_src order by 1 desc limit 1;
insert into pacd_tr_named (b,a)
select transform(a,b) using 'cat' as (x int,y int) from pacd_tr_src order by x desc limit 1;
select 'position', a,b from pacd_tr_pos;
-- the second reduce sink for the LIMIT sorts on the script output too
explain insert into pacd_tr_pos (b,a)
select transform(a,b) using 'cat' as (x int,y int) from pacd_tr_src sort by 1 desc limit 1;
select 'named', a,b from pacd_tr_named;

-- Each multi-insert destination uses its own script output.
create table pacd_tr_mi_a (a int);
create table pacd_tr_mi_b (a int);
from pacd_tr_src
insert into pacd_tr_mi_a (a) select transform(a) using 'cat' as (x int) order by 1 desc limit 1
insert into pacd_tr_mi_b (a) select transform(b) using 'cat' as (x int) order by 1 desc limit 1;
select 'multi_a',a from pacd_tr_mi_a;
select 'multi_b',a from pacd_tr_mi_b;
