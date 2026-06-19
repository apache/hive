create table t(a integer);
create table t2(b integer);

insert into t values (1),(2),(3),(4);
insert into t2 values (1),(2),(3),(4);

explain
select * from t,t2 where
	a*a=b+3
	and
	a in (1,2,3,4)
	and
	b in (1,2,3,4)

	and (
		(a in (1,2) and b in (1,2) ) or 
		(a in (2,3) and b in (2,3) )
			)
	;

