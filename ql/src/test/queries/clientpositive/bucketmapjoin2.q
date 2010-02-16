CREATE TABLE srcbucket_mapjoin(key int, value string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
load data local inpath '../data/files/srcbucket20.txt' INTO TABLE srcbucket_mapjoin;
load data local inpath '../data/files/srcbucket21.txt' INTO TABLE srcbucket_mapjoin;

CREATE TABLE srcbucket_mapjoin_part (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
load data local inpath '../data/files/srcbucket20.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../data/files/srcbucket21.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../data/files/srcbucket22.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../data/files/srcbucket23.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');

CREATE TABLE srcbucket_mapjoin_part_2 (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
load data local inpath '../data/files/srcbucket22.txt' INTO TABLE srcbucket_mapjoin_part_2 partition(ds='2008-04-08');
load data local inpath '../data/files/srcbucket23.txt' INTO TABLE srcbucket_mapjoin_part_2 partition(ds='2008-04-08');


set hive.optimize.bucketmapjoin = true;
create table bucketmapjoin_tmp_result (key string , value1 string, value2 string);


explain extended
insert overwrite table bucketmapjoin_tmp_result 
select /*+mapjoin(b)*/ a.key, a.value, b.value 
from srcbucket_mapjoin a join srcbucket_mapjoin_part_2 b 
on a.key=b.key and b.ds="2008-04-08";

insert overwrite table bucketmapjoin_tmp_result 
select /*+mapjoin(b)*/ a.key, a.value, b.value 
from srcbucket_mapjoin a join srcbucket_mapjoin_part_2 b 
on a.key=b.key and b.ds="2008-04-08";

select count(1) from bucketmapjoin_tmp_result;


explain extended
insert overwrite table bucketmapjoin_tmp_result 
select /*+mapjoin(a)*/ a.key, a.value, b.value 
from srcbucket_mapjoin a join srcbucket_mapjoin_part_2 b 
on a.key=b.key and b.ds="2008-04-08";

insert overwrite table bucketmapjoin_tmp_result 
select /*+mapjoin(a)*/ a.key, a.value, b.value 
from srcbucket_mapjoin a join srcbucket_mapjoin_part_2 b 
on a.key=b.key and b.ds="2008-04-08";

select count(1) from bucketmapjoin_tmp_result;

drop table bucketmapjoin_tmp_result;
drop table srcbucket_mapjoin;
drop table srcbucket_mapjoin_part;
drop table srcbucket_mapjoin_part_2;