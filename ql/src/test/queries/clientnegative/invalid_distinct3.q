--! qt:dataset:src
set hive.cbo.enable=false;
explain select hash(upper(distinct value)) from src;
