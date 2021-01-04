
set hive.auto.convert.join=true;

create table tu  ( a integer );
create table tv  ( b integer );

insert into tu values (1),(2),(3),(4),(5);
insert into tv values (1),(2),(3),(4),(5);


explain
with t as (
  select a+b as s,a+b-abs(a-b) as g from tu,tv where a>1 and b>2 and a*a=b
), k1 as (
  select t.* from t, tu where t.s>tu.a
), k2 as (
  select t.* from t, tv where t.s>tv.b
)
select * from k1,k2 where k1.s=k2.s
;

