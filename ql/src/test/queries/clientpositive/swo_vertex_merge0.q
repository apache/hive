
set hive.auto.convert.join=true;

create table tu  ( a integer );
create table tv  ( b integer );

insert into tu values (1),(2),(3),(4),(5);
insert into tv values (1),(2),(3),(4),(5);


explain
with t as (
	select a+b as s from tu,tv where a>1 and b>2 and a*a=b
)
select sum(t1.s) from t as t1, t as t2 where t1.s=t2.s+1
union all
select sum(t1.s) from t as t1, t as t2 where t1.s+1=t2.s;

