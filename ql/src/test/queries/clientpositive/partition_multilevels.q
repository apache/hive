--! qt:dataset:srcpart
set hive.mapred.mode=nonstrict;
create table partition_test_multilevel (key string, value string) partitioned by (level1 string, level2 string, level3 string);

insert overwrite table partition_test_multilevel partition(level1='1111', level2='111', level3='11') select key, value from srcpart tablesample (11 rows);
insert overwrite table partition_test_multilevel partition(level1='1111', level2='111', level3='22') select key, value from srcpart tablesample (12 rows);
insert overwrite table partition_test_multilevel partition(level1='1111', level2='111', level3='33') select key, value from srcpart tablesample (13 rows);
insert overwrite table partition_test_multilevel partition(level1='1111', level2='111', level3='44') select key, value from srcpart tablesample (14 rows);

insert overwrite table partition_test_multilevel partition(level1='1111', level2='222', level3='11') select key, value from srcpart tablesample (15 rows);
insert overwrite table partition_test_multilevel partition(level1='1111', level2='222', level3='22') select key, value from srcpart tablesample (16 rows);
insert overwrite table partition_test_multilevel partition(level1='1111', level2='222', level3='33') select key, value from srcpart tablesample (17 rows);
insert overwrite table partition_test_multilevel partition(level1='1111', level2='222', level3='44') select key, value from srcpart tablesample (18 rows);

insert overwrite table partition_test_multilevel partition(level1='1111', level2='333', level3='11') select key, value from srcpart tablesample (19 rows);
insert overwrite table partition_test_multilevel partition(level1='1111', level2='333', level3='22') select key, value from srcpart tablesample (20 rows);
insert overwrite table partition_test_multilevel partition(level1='1111', level2='333', level3='33') select key, value from srcpart tablesample (21 rows);
insert overwrite table partition_test_multilevel partition(level1='1111', level2='333', level3='44') select key, value from srcpart tablesample (22 rows);

insert overwrite table partition_test_multilevel partition(level1='2222', level2='111', level3='11') select key, value from srcpart tablesample (11 rows);
insert overwrite table partition_test_multilevel partition(level1='2222', level2='111', level3='22') select key, value from srcpart tablesample (12 rows);
insert overwrite table partition_test_multilevel partition(level1='2222', level2='111', level3='33') select key, value from srcpart tablesample (13 rows);
insert overwrite table partition_test_multilevel partition(level1='2222', level2='111', level3='44') select key, value from srcpart tablesample (14 rows);

insert overwrite table partition_test_multilevel partition(level1='2222', level2='222', level3='11') select key, value from srcpart tablesample (15 rows);
insert overwrite table partition_test_multilevel partition(level1='2222', level2='222', level3='22') select key, value from srcpart tablesample (16 rows);
insert overwrite table partition_test_multilevel partition(level1='2222', level2='222', level3='33') select key, value from srcpart tablesample (17 rows);
insert overwrite table partition_test_multilevel partition(level1='2222', level2='222', level3='44') select key, value from srcpart tablesample (18 rows);

insert overwrite table partition_test_multilevel partition(level1='2222', level2='333', level3='11') select key, value from srcpart tablesample (19 rows);
insert overwrite table partition_test_multilevel partition(level1='2222', level2='333', level3='22') select key, value from srcpart tablesample (20 rows);
insert overwrite table partition_test_multilevel partition(level1='2222', level2='333', level3='33') select key, value from srcpart tablesample (21 rows);
insert overwrite table partition_test_multilevel partition(level1='2222', level2='333', level3='44') select key, value from srcpart tablesample (22 rows);

set metaconf:hive.metastore.try.direct.sql=false;

-- beginning level partition in predicate
select level1, level2, level3, count(*) from partition_test_multilevel where level1 = '2222' group by level1, level2, level3;
select level1, level2, level3, count(*) from partition_test_multilevel where level1 >= '2222' group by level1, level2, level3;
select level1, level2, level3, count(*) from partition_test_multilevel where level1 !='2222' group by level1, level2, level3;

-- middle level partition in predicate
select level1, level2, level3, count(*) from partition_test_multilevel where level2 = '222' group by level1, level2, level3;
select level1, level2, level3, count(*) from partition_test_multilevel where level2 <= '222' group by level1, level2, level3;
select level1, level2, level3, count(*) from partition_test_multilevel where level2 != '222' group by level1, level2, level3;

-- ending level partition in predicate
select level1, level2, level3, count(*) from partition_test_multilevel where level3 = '22' group by level1, level2, level3;
select level1, level2, level3, count(*) from partition_test_multilevel where level3 >= '22' group by level1, level2, level3;
select level1, level2, level3, count(*) from partition_test_multilevel where level3 != '22' group by level1, level2, level3;

-- two different levels of partitions in predicate
select level1, level2, level3, count(*) from partition_test_multilevel where level2 >= '222' and level3 = '33' group by level1, level2, level3;

select level1, level2, level3, count(*) from partition_test_multilevel where level1 <= '1111' and level3 >= '33' group by level1, level2, level3;


-- all levels of partitions in predicate
select level1, level2, level3, count(*) from partition_test_multilevel where level1 = '2222' and level2 >= '222' and level3 <= '33' group by level1, level2, level3;

-- between
select level1, level2, level3, count(*) from partition_test_multilevel where (level1 = '2222') and (level2 between '222' and '333') and (level3 between '11' and '33') group by level1, level2, level3;

explain select level1, level2, level3, count(*) from partition_test_multilevel where (level1 = '2222') and (level2 between '222' and '333') and (level3 between '11' and '33') group by level1, level2, level3;

set metaconf:hive.metastore.try.direct.sql=true;

-- beginning level partition in predicate
select level1, level2, level3, count(*) from partition_test_multilevel where level1 = '2222' group by level1, level2, level3;
select level1, level2, level3, count(*) from partition_test_multilevel where level1 >= '2222' group by level1, level2, level3;
select level1, level2, level3, count(*) from partition_test_multilevel where level1 !='2222' group by level1, level2, level3;

-- middle level partition in predicate
select level1, level2, level3, count(*) from partition_test_multilevel where level2 = '222' group by level1, level2, level3;
select level1, level2, level3, count(*) from partition_test_multilevel where level2 <= '222' group by level1, level2, level3;
select level1, level2, level3, count(*) from partition_test_multilevel where level2 != '222' group by level1, level2, level3;

-- ending level partition in predicate
select level1, level2, level3, count(*) from partition_test_multilevel where level3 = '22' group by level1, level2, level3;
select level1, level2, level3, count(*) from partition_test_multilevel where level3 >= '22' group by level1, level2, level3;
select level1, level2, level3, count(*) from partition_test_multilevel where level3 != '22' group by level1, level2, level3;

-- two different levels of partitions in predicate
select level1, level2, level3, count(*) from partition_test_multilevel where level2 >= '222' and level3 = '33' group by level1, level2, level3;

select level1, level2, level3, count(*) from partition_test_multilevel where level1 <= '1111' and level3 >= '33' group by level1, level2, level3;


-- all levels of partitions in predicate
select level1, level2, level3, count(*) from partition_test_multilevel where level1 = '2222' and level2 >= '222' and level3 <= '33' group by level1, level2, level3;

-- between
select level1, level2, level3, count(*) from partition_test_multilevel where (level1 = '2222') and (level2 between '222' and '333') and (level3 between '11' and '33') group by level1, level2, level3;

explain select level1, level2, level3, count(*) from partition_test_multilevel where (level1 = '2222') and (level2 between '222' and '333') and (level3 between '11' and '33') group by level1, level2, level3;
