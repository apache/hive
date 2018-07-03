--! qt:dataset:src

set spark.executor.cores=-1;

select * from src order by key limit 10;
