
create table tu  ( a integer );
create table tv  ( b integer );

insert into tu values (1),(2),(3),(4),(5);
insert into tv values (1),(2),(3),(4),(5);


explain
with t as (
	select a+b as s from tu,tv where a>1 and b>2
)
select t1.s=t2.s+1 from t as t1, t as t2;

