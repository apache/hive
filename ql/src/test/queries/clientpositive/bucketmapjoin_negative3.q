set hive.strict.checks.bucketing=false;

drop table test1_n11;
drop table test2_n7;
drop table test3;
drop table test4;

create table test1_n11 (key string, value string) clustered by (key) sorted by (key) into 3 buckets;
create table test2_n7 (key string, value string) clustered by (value) sorted by (value) into 3 buckets;
create table test3 (key string, value string) clustered by (key, value) sorted by (key, value) into 3 buckets;
create table test4 (key string, value string) clustered by (value, key) sorted by (value, key) into 3 buckets;

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE test1_n11;
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE test1_n11;
load data local inpath '../../data/files/bmj/000002_0' INTO TABLE test1_n11;

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE test2_n7;
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE test2_n7;
load data local inpath '../../data/files/bmj/000002_0' INTO TABLE test2_n7;

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE test3;
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE test3;
load data local inpath '../../data/files/bmj/000002_0' INTO TABLE test3;

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE test4;
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE test4;
load data local inpath '../../data/files/bmj/000002_0' INTO TABLE test4;
set hive.cbo.enable=false;
set hive.optimize.bucketmapjoin = true;
-- should be allowed
explain extended select /*+ MAPJOIN(R) */ * from test1_n11 L join test1_n11 R on L.key=R.key AND L.value=R.value;
explain extended select /*+ MAPJOIN(R) */ * from test2_n7 L join test2_n7 R on L.key=R.key AND L.value=R.value;

-- should not apply bucket mapjoin
explain extended select /*+ MAPJOIN(R) */ * from test1_n11 L join test1_n11 R on L.key+L.key=R.key;
explain extended select /*+ MAPJOIN(R) */ * from test1_n11 L join test2_n7 R on L.key=R.key AND L.value=R.value;
explain extended select /*+ MAPJOIN(R) */ * from test1_n11 L join test3 R on L.key=R.key AND L.value=R.value;
explain extended select /*+ MAPJOIN(R) */ * from test1_n11 L join test4 R on L.key=R.key AND L.value=R.value;
explain extended select /*+ MAPJOIN(R) */ * from test2_n7 L join test3 R on L.key=R.key AND L.value=R.value;
explain extended select /*+ MAPJOIN(R) */ * from test2_n7 L join test4 R on L.key=R.key AND L.value=R.value;
explain extended select /*+ MAPJOIN(R) */ * from test3 L join test4 R on L.key=R.key AND L.value=R.value;
