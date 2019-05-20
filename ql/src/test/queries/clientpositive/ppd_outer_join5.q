set hive.mapred.mode=nonstrict;
set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;

create table t1_n98 (id int, key string, value string);
create table t2_n61 (id int, key string, value string);
create table t3_n23 (id int, key string, value string);
create table t4_n12 (id int, key string, value string);

explain select * from t1_n98 full outer join t2_n61 on t1_n98.id=t2_n61.id join t3_n23 on t2_n61.id=t3_n23.id where t3_n23.id=20;
explain select * from t1_n98 join t2_n61 on (t1_n98.id=t2_n61.id) left outer join t3_n23 on (t2_n61.id=t3_n23.id) where t2_n61.id=20;
explain select * from t1_n98 join t2_n61 on (t1_n98.id=t2_n61.id) left outer join t3_n23 on (t1_n98.id=t3_n23.id) where t2_n61.id=20;

drop table t1_n98;
drop table t2_n61;
drop table t3_n23;
drop table t4_n12;