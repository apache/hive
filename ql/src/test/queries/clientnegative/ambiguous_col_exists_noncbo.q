--! qt:dataset:src
set hive.cbo.enable=false;
select key from src a where exists (select 'x' as c, 'y' as c from src b where b.key = a.key);
