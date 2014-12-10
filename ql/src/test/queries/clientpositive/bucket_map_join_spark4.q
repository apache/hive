set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;
set hive.exec.reducers.max = 1;

-- SORT_QUERY_RESULTS

CREATE TABLE tbl1(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE tbl2(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE tbl3(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;

insert overwrite table tbl1
select * from src where key < 10;

insert overwrite table tbl2
select * from src where key < 10;

insert overwrite table tbl3
select * from src where key < 10;

set hive.enforce.bucketing = false;
set hive.enforce.sorting = false;
set hive.exec.reducers.max = 100;

set hive.auto.convert.join=true;

set hive.optimize.bucketmapjoin = true;

explain extended
select a.key as key, a.value as val1, b.value as val2, c.value as val3
from tbl1 a join tbl2 b on a.key = b.key join tbl3 c on a.value = c.value;

select a.key as key, a.value as val1, b.value as val2, c.value as val3
from tbl1 a join tbl2 b on a.key = b.key join tbl3 c on a.value = c.value;

set hive.optimize.bucketmapjoin = false;

explain extended
select a.key as key, a.value as val1, b.value as val2, c.value as val3
from tbl1 a join tbl2 b on a.key = b.key join tbl3 c on a.value = c.value;

select a.key as key, a.value as val1, b.value as val2, c.value as val3
from tbl1 a join tbl2 b on a.key = b.key join tbl3 c on a.value = c.value;
