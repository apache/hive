set hive.explain.user=false;

drop table if exists tbl_ice;
create external table tbl_ice(a int, b string, c int) stored by iceberg tblproperties ('format-version'='2', 'write.delete.mode'='copy-on-write');

-- delete using simple predicates
insert into tbl_ice values (1, 'one', 50), (2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54), (111, 'one', 55), (333, 'two', 56);
explain delete from tbl_ice where b in ('one', 'four') or a = 22;

delete from tbl_ice where b in ('one', 'four') or a = 22;
select * from tbl_ice order by a;
-- (2, 'two', 51), (3, 'three', 52),  (5, 'five', 54), (333, 'two', 56)

-- delete using subqueries referencing the same table
insert into tbl_ice values (444, 'hola', 800), (555, 'schola', 801);
explain delete from tbl_ice where a in (select a from tbl_ice where a <= 5) or c in (select c from tbl_ice where c > 800);

delete from tbl_ice where a in (select a from tbl_ice where a <= 5) or c in (select c from tbl_ice where c > 800);
select * from tbl_ice order by a;
-- (333, 'two', 56), (444, 'hola', 800)

-- delete using a join subquery between the same table & another table
drop table if exists tbl_ice_other;
create external table tbl_ice_other(a int, b string) stored by iceberg;
insert into tbl_ice_other values (10, 'ten'), (333, 'hundred');
explain delete from tbl_ice where a in (select t1.a from tbl_ice t1 join tbl_ice_other t2 on t1.a = t2.a);

delete from tbl_ice where a in (select t1.a from tbl_ice t1 join tbl_ice_other t2 on t1.a = t2.a);
select * from tbl_ice order by a;
-- (444, 'hola', 800)

-- delete using a join subquery between the same table & a non-iceberg table
drop table if exists tbl_standard_other;
create external table tbl_standard_other(a int, b string) stored as orc;
insert into tbl_standard_other values (10, 'ten'), (444, 'tutu');
explain delete from tbl_ice where a in (select t1.a from tbl_ice t1 join tbl_standard_other t2 on t1.a = t2.a);

delete from tbl_ice where a in (select t1.a from tbl_ice t1 join tbl_standard_other t2 on t1.a = t2.a);
select count(*) from tbl_ice;
-- 0

-- null cases
drop table if exists tbl_ice_with_nulls;
create table tbl_ice_with_nulls (id int, name string) stored by iceberg tblproperties('format-version'='2', 'write.delete.mode'='copy-on-write');

insert into tbl_ice_with_nulls values
(1, 'ABC'),(2, 'CBS'),(3, null),(4, 'POPI'),(5, 'AQWR'),(6, 'POIU'),(7, 'SDF'),(9, null),(8,'POIKL'),(10, 'YUIO');

delete from tbl_ice_with_nulls where id in (select id from tbl_ice_with_nulls where id > 9) or name in (select name from tbl_ice_with_nulls where name = 'sdf');
select * from tbl_ice_with_nulls order by id;

-- delete without where clause and disabled conversion to truncate
set hive.optimize.delete.all=false;
delete from tbl_ice_with_nulls;
select * from tbl_ice_with_nulls;