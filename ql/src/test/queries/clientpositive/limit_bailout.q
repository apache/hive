-- this test should run in TestMiniLlapCliDriver (not local) as it validates HIVE-25751,
-- that typically reproduces on DFSClient codepath
create table limit_bailout_src_text(c string);
load data local inpath '../../data/files/smbdata.txt' into table limit_bailout_src_text;
create table limit_bailout(c string) clustered by (c) sorted by (c) into 5 buckets;
insert overwrite table limit_bailout select * from limit_bailout_src_text;
select 1 from limit_bailout join limit_bailout t1 on limit_bailout.c=t1.c limit 1;
