create table t1_n32( key_int1_n32 int, key_int2 int, key_string1 string, key_string2 string);

set hive.optimize.reducededuplication=false;

set hive.map.aggr=false;
select Q1.key_int1_n32, sum(Q1.key_int1_n32) from (select * from t1_n32 cluster by key_int1_n32) Q1 group by Q1.key_int1_n32;

drop table t1_n32;
