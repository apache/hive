-- disable cbo because calcite can see thru these test cases; the goal here is to test the annotation processing
set hive.cbo.enable=false;

set hive.semantic.analyzer.hook=org.apache.hadoop.hive.ql.hooks.AccurateEstimatesCheckerHook;
set accurate.estimate.checker.absolute.error=5;
 
set hive.explain.user=true;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;

drop table if exists t1;
drop table if exists t8;

create table t1 (a integer,b integer);
create table t8 like t1;

insert into t1 values (1,1),(2,2),(3,3),(4,4),(5,5);

insert into t8
select * from t1 union all select * from t1 union all select * from t1 union all select * from t1 union all
select * from t1 union all select * from t1 union all select * from t1 union all select * from t1
;

analyze table t1 compute statistics for columns;
analyze table t8 compute statistics for columns;

explain analyze select sum(a) from t8 where b in (2,3) group by b;
explain analyze select sum(a) from t8 where b=2 group by b;

explain analyze select sum(a) from t1 where 2=b and b=2 group by b;

explain analyze select sum(a) from t1 where b in (2,3) and b=2 group by b;
explain analyze select sum(a) from t8 where b in (2,3) and b=2 group by b;


explain analyze select count(*) from t8 ta, t8 tb where ta.a = tb.b and ta.a=3;


explain analyze select sum(a) from t8 where b in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50) and b=2 and b=2 and 2=b group by b;

explain analyze select sum(a) from t8 where b=2 and (b = 1 or b=2) group by b;

set accurate.estimate.checker.absolute.error=8;

explain analyze select sum(a) from t8 where b=2 and (b = 1 or b=2) and (b=1 or b=3) group by b;

explain analyze select sum(a) from t8 where
	b=2 and (b = 1 or b=2)
and
	a=3 and (a = 3 or a=4)
group by b;

