create table t1 (key int, value int);

set hive.optimize.cte.materialize.threshold=1;
set hive.optimize.cte.suggester.class=org.apache.hadoop.hive.ql.optimizer.calcite.CommonTableExpressionIdentitySuggester;
set hive.optimize.cte.materialize.full.aggregate.only=false;

explain cbo
select *
from (select key from t1 where value = 10) a
join (select key from t1 where value = 10) b;
