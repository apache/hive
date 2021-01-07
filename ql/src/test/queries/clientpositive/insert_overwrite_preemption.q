set hive.explain.user=false;
set hive.stats.autogather=true;
CREATE TABLE sourceTable (one string,two string);
load data local inpath '../../data/files/kv1.txt' INTO TABLE sourceTable;
load data local inpath '../../data/files/kv3.txt' INTO TABLE sourceTable;
CREATE TABLE destinTable (two string) partitioned by (one string);
EXPLAIN INSERT OVERWRITE TABLE destinTable partition (one) SELECT one,two FROM sourceTable;
drop table destinTable;
drop table sourceTable;
