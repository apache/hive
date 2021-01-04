
set hive.auto.convert.join=true;

create table tu  ( a integer );
create table tv  ( b integer );

insert into tu values (1),(2),(3),(4),(5);
insert into tv values (1),(2),(3),(4),(5);


explain
with t as (
  select a+b as s,a+b-abs(a-b) as g from tu,tv where a>1 and b>2 and a*a=b
), k1 as (
  select t1.g as g,t1.s as s from t as t1, t as t2 where t1.s=t2.s+1
), k2 as (
  select t1.g as g,t1.s as s from t as t1, t as t2 where t1.s+1=t2.s
)
select k1.g,k1.s,k2.s from k1 as k1 ,k2 as k2 where k1.g=k2.g
;

