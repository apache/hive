set hive.stats.column.autogather=false;
set hive.strict.checks.bucketing=true;

set hive.explain.user=false;

-- SORT_QUERY_RESULTS

-- Single key partition
-- Load with full partition spec
CREATE TABLE src_bucket_tbl(key int, value string) partitioned by (ds string) clustered by (key) into 1 buckets STORED AS TEXTFILE;
explain load data local inpath '../../data/files/bmj/000000_0' INTO TABLE src_bucket_tbl partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000000_0' INTO TABLE src_bucket_tbl partition(ds='2008-04-08');
select * from src_bucket_tbl where ds='2008-04-08';

drop table src_bucket_tbl;

-- Multi key partition
-- Load with both static and dynamic partition spec where dynamic partition value is not in file and expected to use default partition.
CREATE TABLE src_bucket_tbl(key int, value string) partitioned by (hr int, ds string) clustered by (key) into 1 buckets STORED AS TEXTFILE;
explain load data local inpath '../../data/files/bmj/000000_0' INTO TABLE src_bucket_tbl partition(ds, hr=10);
load data local inpath '../../data/files/bmj/000000_0' INTO TABLE src_bucket_tbl partition(ds, hr=10);
select * from src_bucket_tbl where hr=10;

drop table src_bucket_tbl;

-- Multi key partition
-- Load with both static and dynamic partition spec where dynamic partition value present in file.
CREATE TABLE src_bucket_tbl(key int, value string) partitioned by (hr int, ds string) clustered by (key) into 1 buckets STORED AS TEXTFILE;
explain load data local inpath '../../data/files/load_data_job/load_data_1_partition.txt' INTO TABLE src_bucket_tbl partition(hr=20, ds);
load data local inpath '../../data/files/load_data_job/load_data_1_partition.txt' INTO TABLE src_bucket_tbl partition(hr=20, ds);
select * from src_bucket_tbl where hr=20 and ds='2008-04-08';

drop table src_bucket_tbl;

-- Multi key partition
-- Load with both static and dynamic partition spec
CREATE TABLE src_bucket_tbl(key int, value string) partitioned by (hr int, ds string) clustered by (key) into 1 buckets STORED AS TEXTFILE;
explain load data local inpath '../../data/files/bmj/000000_0' INTO TABLE src_bucket_tbl partition(hr=30, ds='2010-05-07');
load data local inpath '../../data/files/bmj/000000_0' INTO TABLE src_bucket_tbl partition(hr=30, ds='2010-05-07');
select * from src_bucket_tbl where hr=30 and ds='2010-05-07';

drop table src_bucket_tbl;