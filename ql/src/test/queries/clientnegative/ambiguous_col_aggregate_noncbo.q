--! qt:dataset:src
set hive.cbo.enable=false;
select count(*) from (select key, key from src) subq;
