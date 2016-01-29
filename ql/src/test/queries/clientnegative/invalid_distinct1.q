set hive.cbo.enable=false;
explain select hash(distinct value) from src;
