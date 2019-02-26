--! qt:dataset:src

set hive.cbo.enable=false;

select distinct count(value) from src group by key
