--! qt:dataset:src

set spark.executor.memory=-1;

select * from src order by key limit 10;
