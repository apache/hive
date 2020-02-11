--! qt:dataset:src
set hive.cbo.enable=false;
explain select hash(distinct value) from src;
