--! qt:dataset:src
set hive.cbo.enable=false;
select value, count(1) from src group by value having exists (select 'x' as c, 'y' as c from src b where b.value = src.value);
