CREATE TABLE T1_n134(key1 STRING, val1 STRING) STORED AS TEXTFILE;
CREATE TABLE T2_n80(key2 STRING, val2 STRING) STORED AS TEXTFILE;
CREATE TABLE T3_n32(key3 STRING, val3 STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n134;
LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2_n80;
LOAD DATA LOCAL INPATH '../../data/files/T3.txt' INTO TABLE T3_n32;

set hive.cbo.enable=false;

explain select /*+ STREAMTABLE(a) */ a.*, b.*, c.* from T1_n134 a join T2_n80 b on a.key1=b.key2 join T3_n32 c on a.key1=c.key3;
select /*+ STREAMTABLE(a) */ a.*, b.*, c.* from T1_n134 a join T2_n80 b on a.key1=b.key2 join T3_n32 c on a.key1=c.key3;

explain select /*+ STREAMTABLE(b) */ a.*, b.*, c.* from T1_n134 a join T2_n80 b on a.key1=b.key2 join T3_n32 c on a.key1=c.key3;
select /*+ STREAMTABLE(b) */ a.*, b.*, c.* from T1_n134 a join T2_n80 b on a.key1=b.key2 join T3_n32 c on a.key1=c.key3;

explain select /*+ STREAMTABLE(c) */ a.*, b.*, c.* from T1_n134 a join T2_n80 b on a.key1=b.key2 join T3_n32 c on a.key1=c.key3;
select /*+ STREAMTABLE(c) */ a.*, b.*, c.* from T1_n134 a join T2_n80 b on a.key1=b.key2 join T3_n32 c on a.key1=c.key3;
