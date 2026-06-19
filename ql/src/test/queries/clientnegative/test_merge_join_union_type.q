set hive.cbo.enable=false;
set hive.auto.convert.join=false;
set hive.optimize.ppd=false;
create table test_union1 (key int, value UNIONTYPE<int, string>, col_1 STRING) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS;
SELECT * FROM test_union1 t1 INNER JOIN test_union1 t2 ON t1.value = t2.value;