set hive.mapred.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=strict;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

create external table sdp (
  dataint bigint,
  hour int,
  req string,
  cid string,
  caid string
)
row format delimited
fields terminated by ',';

load data local inpath '../../data/files/dynpartdata1.txt' into table sdp;
load data local inpath '../../data/files/dynpartdata2.txt' into table sdp;

create table tdp (cid string, caid string)
partitioned by (dataint bigint, hour int, req string);

insert overwrite table tdp partition (dataint=20150316, hour=16, req)
select cid, caid, req from sdp where dataint=20150316 and hour=16;

select * from tdp order by caid;
show partitions tdp;
