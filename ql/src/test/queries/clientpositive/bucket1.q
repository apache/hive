set hive.enforce.bucketing = true;
set hive.exec.reducers.max = 200;

drop table bucket1_1;
CREATE TABLE bucket1_1(key int, value string) CLUSTERED BY (key) INTO 100 BUCKETS;

explain extended
insert overwrite table bucket1_1
select * from src;

insert overwrite table bucket1_1
select * from src;

select * from bucket1_1 order by key;

drop table bucket1_1;
