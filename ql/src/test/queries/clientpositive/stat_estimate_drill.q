set hive.explain.user=true;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;

drop table if exists t1;
drop table if exists t8;

create table t1 (a integer);
create table t3 (a integer,b integer,c integer);

insert into t1 values (1),(2),(3),(4),(5),(6),(7),(8),(9),(0);
insert into t3
	select x1.a as a,x2.a as b,x3.a as c from
		t1 x1
		join t1 x2
		join t1 x3;

analyze table t3 compute statistics for columns;

explain analyze select sum(a) from t3 where b in (2,3) group by b;

explain analyze select sum(a) from t3 where a=1 or a=2 group by b;
explain analyze select sum(a) from t3 where a=1 or (a=2  and b=3) group by b;
explain analyze select sum(a) from t3 where a=1 group by b;
explain analyze select sum(a) from t3 where a=1 and b=2 group by b;
explain analyze select sum(a) from t3 where a=1 and b=2 and c=3 group by b;

explain analyze select sum(a) from t3 where (a=1 and b=2) or (a=2 and b=3) or (a=3 and b=4) group by b;
