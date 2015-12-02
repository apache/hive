set hive.mapred.mode=nonstrict;
set hive.cbo.enable=true;
set hive.exec.check.crossproducts=false;

set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join=false;

-- 20. Test get stats with empty partition list
select cbo_t1.value from cbo_t1 join cbo_t2 on cbo_t1.key = cbo_t2.key where cbo_t1.dt = '10' and cbo_t1.c_boolean = true;

