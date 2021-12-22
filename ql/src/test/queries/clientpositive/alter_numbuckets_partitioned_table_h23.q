--! qt:dataset:src
create table tst1_n1(key string, value string) partitioned by (ds string) clustered by (key) into 10 buckets;

alter table tst1_n1 clustered by (key) into 8 buckets;

describe formatted tst1_n1;


insert overwrite table tst1_n1 partition (ds='1') select key, value from src;

describe formatted tst1_n1 partition (ds = '1');

-- Test changing bucket number

alter table tst1_n1 clustered by (key) into 12 buckets;

insert overwrite table tst1_n1 partition (ds='1') select key, value from src;

describe formatted tst1_n1 partition (ds = '1');

describe formatted tst1_n1;

-- Test changing bucket number of (table/partition)

alter table tst1_n1 into 4 buckets;

describe formatted tst1_n1;

describe formatted tst1_n1 partition (ds = '1');

alter table tst1_n1 partition (ds = '1') into 6 buckets;

describe formatted tst1_n1;

describe formatted tst1_n1 partition (ds = '1');

-- Test adding sort order

alter table tst1_n1 clustered by (key) sorted by (key asc) into 12 buckets;

describe formatted tst1_n1;

-- Test changing sort order

alter table tst1_n1 clustered by (key) sorted by (value desc) into 12 buckets;

describe formatted tst1_n1;

-- Test removing test order

alter table tst1_n1 clustered by (value) into 12 buckets;

describe formatted tst1_n1;

-- Test changing name of bucket column

alter table tst1_n1 change key keys string;

describe formatted tst1_n1;

-- Test removing buckets

alter table tst1_n1 not clustered;

describe formatted tst1_n1;
