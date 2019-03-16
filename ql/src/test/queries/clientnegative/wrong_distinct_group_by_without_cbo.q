--! qt:dataset:src

set hive.cbo.enable=false;

select distinct key from src group by key
