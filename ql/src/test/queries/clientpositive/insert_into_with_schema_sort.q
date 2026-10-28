-- HIVE-30037: in an INSERT with a column list, sort and partition keys refer to the select list,
-- by position or by name, whether CBO plans the statement or not.
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.exec.dynamic.partition.mode=nonstrict;

create table iws_src (x int, y int, p string);
insert into iws_src values (1, 20, 'a'), (3, 10, 'b'), (2, 30, 'a');
create table iws_t (x int, y int);
create table iws_t2 (x int, y int);
create table iws_src2 (x int, y int);
insert into iws_src2 values (1, 20), (3, 10), (2, 30);
create table iws_p (x int, y int) partitioned by (p string);
create table iws_acid (x int, y int) stored as orc tblproperties ('transactional'='true');

insert into iws_t (y, x) select x, y from iws_src order by 1 desc limit 1;
select x, y from iws_t;
truncate table iws_t;
insert into iws_t (y, x) select x, y from iws_src order by x desc limit 1;
select x, y from iws_t;
truncate table iws_t;
insert into iws_t (y, x) select s.x, s.y from iws_src s order by s.y desc limit 1;
select x, y from iws_t;
truncate table iws_t;
insert into iws_t (y, x) select x as a, y from iws_src order by a desc limit 1;
select x, y from iws_t;
truncate table iws_t;
explain insert into iws_t (y, x) select x, y from iws_src distribute by x sort by y desc;
-- with a LIMIT, an INSERT gets a second reduce sink that sorts and partitions on the same select list columns
explain insert into iws_t (y, x) select x, y from iws_src distribute by x sort by y desc limit 1;
explain insert into iws_t (y, x) select a.x, b.y from iws_src a join iws_src2 b on a.x = b.x sort by b.y desc limit 1;
insert into iws_t (y, x) select x, y from iws_src sort by 1 desc limit 1;
select x, y from iws_t;
truncate table iws_t;
insert into iws_p partition (p) (y, x, p) select x, y, p from iws_src order by x desc limit 1;
select x, y, p from iws_p;
insert into iws_acid (y, x) select x, y from iws_src order by 2 desc limit 1;
select x, y from iws_acid;

explain insert into iws_p partition (p) (y, x, p) select x, y, p from iws_src distribute by p sort by x desc;
insert into iws_p partition (p) (y, x, p) select x, y, p from iws_src distribute by p sort by x desc;
select x, y, p from iws_p;
insert into iws_p partition (p = 'k') (y, x) select x, y from iws_src order by 1 desc limit 1;
select x, y, p from iws_p where p = 'k';
insert into iws_t (y, x) select * from iws_src2 order by 1 desc limit 1;
select x, y from iws_t;
truncate table iws_t;
insert into iws_t (y, x) select a.x, b.y from iws_src a join iws_src2 b on a.x = b.x order by a.x desc limit 1;
select x, y from iws_t;
truncate table iws_t;
insert into iws_t (y, x) select a.x, b.y from iws_src a join iws_src2 b on a.x = b.x order by b.y desc limit 1;
select x, y from iws_t;
truncate table iws_t;
-- as in a plain select, a duplicate alias refers to the last column with that alias
insert into iws_t (y, x) select x, y as x from iws_src order by x desc limit 1;
select x, y from iws_t;
truncate table iws_t;
with c as (select x, y from iws_src) insert into iws_t (y, x) select x, y from c order by 1 desc limit 1;
select x, y from iws_t;
truncate table iws_t;
set hive.optimize.cte.materialize.threshold=1;
set hive.optimize.cte.materialize.full.aggregate.only=false;
with c as (select x, y from iws_src) insert into iws_t (y, x) select x, y from c order by x desc limit 1;
select x, y from iws_t;
truncate table iws_t;
set hive.optimize.cte.materialize.threshold=-1;
create view iws_v as select x, y from iws_src;
insert into iws_t (y, x) select * from iws_v order by 2 desc limit 1;
select x, y from iws_t;
truncate table iws_t;
truncate table iws_t2;
from iws_src
insert into table iws_t (y, x) select x, y order by 1 desc limit 1
insert into table iws_t2 (y, x) select x, y order by y desc limit 1;
select x, y from iws_t;
select x, y from iws_t2;
truncate table iws_t;
insert into iws_t (y, x) select stack(3, 1, 20, 3, 10, 2, 30) as (a, b) order by 1 desc limit 1;
select x, y from iws_t;
truncate table iws_t;

set hive.cbo.enable=false;
explain insert into iws_t (y, x) select x, y from iws_src sort by y desc limit 1;
insert into iws_t (y, x) select x, y from iws_src order by 1 desc limit 1;
select x, y from iws_t;
truncate table iws_t;
insert into iws_t (y, x) select x, y from iws_src order by y desc limit 1;
select x, y from iws_t;
truncate table iws_t;
insert into iws_t (y, x) select a.x, b.y from iws_src a join iws_src2 b on a.x = b.x order by a.x desc limit 1;
select x, y from iws_t;
truncate table iws_t;
insert into iws_t (y, x) select distinct * from iws_src2;
select x, y from iws_t order by x;
