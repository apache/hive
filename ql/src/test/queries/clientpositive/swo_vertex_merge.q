
create table tu  ( a integer );
create table tv  ( b integer );

insert into tu value (1),(2),(3),(4),(5);
insert into tv value (1),(2),(3),(4),(5);


with t as (
	select a+b as s from tu,tv where a>1 and b>2
)
select t1.s=t2.s+1 from t as t1, t as t2;

