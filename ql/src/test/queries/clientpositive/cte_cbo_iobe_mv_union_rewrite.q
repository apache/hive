set hive.optimize.cte.suggester.class=org.apache.hadoop.hive.ql.optimizer.calcite.CommonTableExpressionIdentitySuggester;
set hive.optimize.cte.materialize.threshold=1;
set hive.optimize.cte.materialize.full.aggregate.only=false;

create table emps
(
    empid      int,
    name       varchar(256)
);

EXPLAIN CBO
SELECT min(name)
from emps
UNION ALL
select distinct(name)
from emps
where name IS NOT NULL
UNION ALL
select distinct(name)
from emps
WHERE name IS NOT NULL;