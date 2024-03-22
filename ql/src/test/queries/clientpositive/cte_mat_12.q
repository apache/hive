create table t0(col0 int);
insert into t0(col0) values
 (1),(2),
 (100),(100),(100),
 (200),(200);


set hive.optimize.cte.suggester.class=org.apache.hadoop.hive.ql.optimizer.calcite.CommonTableExpressionIdentitySuggester;
set hive.optimize.cte.materialize.threshold=1;

set hive.optimize.cte.materialize.full.aggregate.only=true;

explain cbo
select cte1.cnt, cte2.cnt
from (select count(*) as cnt from t0 where col0 < 10) cte1
inner join (select count(*) as cnt from t0 where col0 < 10) cte2
    on cte1.cnt = cte2.cnt;

explain cbo
select cte1.cnt, cte2.cnt, cte1.literal
from (select count(*) as cnt, 'A' as literal from t0 where col0 < 10) cte1
inner join (select count(*) as cnt, 'A' as literal from t0 where col0 < 10) cte2
    on cte1.cnt = cte2.cnt;

explain cbo
select cte1.col0, cte2.col0
from (select col0 from t0 where col0 < 10) cte1
inner join (select col0 from t0 where col0 < 10) cte2
    on cte1.col0 = cte2.col0;

set hive.optimize.cte.materialize.full.aggregate.only=false;

explain cbo
select cte1.col0, cte2.col0
from (select col0 from t0 where col0 < 10) cte1
inner join (select col0 from t0 where col0 < 10) cte2
    on cte1.col0 = cte2.col0;
