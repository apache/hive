set hive.strict.checks.cartesian.product=false;
set hive.materializedview.rewriting=true;
set hive.stats.column.autogather=true;

create table cmv_basetable (a int, b varchar(256), c decimal(10,2), d int);

insert into cmv_basetable values
 (1, 'alfred', 10.30, 2),
 (2, 'bob', 3.14, 3),
 (2, 'bonnie', 172342.2, 3),
 (3, 'calvin', 978.76, 3),
 (3, 'charlie', 9.8, 1);

create materialized view cmv_mat_view enable rewrite
as select a, b, c from cmv_basetable where a = 2;

select * from cmv_mat_view;

show tblproperties cmv_mat_view;

create materialized view if not exists cmv_mat_view2 enable rewrite
as select a, c from cmv_basetable where a = 3;

select * from cmv_mat_view2;

show tblproperties cmv_mat_view2;

explain
select a, c from cmv_basetable where a = 3;

select a, c from cmv_basetable where a = 3;

explain
select * from (
  (select a, c from cmv_basetable where a = 3) table1
  join
  (select a, c from cmv_basetable where d = 3) table2
  on table1.a = table2.a);

select * from (
  (select a, c from cmv_basetable where a = 3) table1
  join
  (select a, c from cmv_basetable where d = 3) table2
  on table1.a = table2.a);

drop materialized view cmv_mat_view2;

explain
select * from (
  (select a, c from cmv_basetable where a = 3) table1
  join
  (select a, c from cmv_basetable where d = 3) table2
  on table1.a = table2.a);

select * from (
  (select a, c from cmv_basetable where a = 3) table1
  join
  (select a, c from cmv_basetable where d = 3) table2
  on table1.a = table2.a);

drop materialized view cmv_mat_view;
