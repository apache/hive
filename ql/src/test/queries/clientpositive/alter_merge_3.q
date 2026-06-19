dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/alter_merge_location/ds20140804;
dfs -put ../../data/files/smbbucket_1.rc ${system:test.tmp.dir}/alter_merge_location/ds20140804;
dfs -put ../../data/files/smbbucket_2.rc ${system:test.tmp.dir}/alter_merge_location/ds20140804;
dfs -put ../../data/files/smbbucket_3.rc ${system:test.tmp.dir}/alter_merge_location/ds20140804;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/alter_merge_location/ds20140805;
dfs -put ../../data/files/smbbucket_1.rc ${system:test.tmp.dir}/alter_merge_location/ds20140805;
dfs -put ../../data/files/smbbucket_2.rc ${system:test.tmp.dir}/alter_merge_location/ds20140805;
dfs -put ../../data/files/smbbucket_3.rc ${system:test.tmp.dir}/alter_merge_location/ds20140805;

create table src_rc_merge_test_part (key int, value string) partitioned by (ds string) stored as rcfile;

set fs.hdfs.impl.disable.cache=false;
alter table src_rc_merge_test_part add partition (ds = '2014-08-04') location '${system:test.tmp.dir}/alter_merge_location/ds20140804';
alter table src_rc_merge_test_part partition (ds = '2014-08-04') concatenate;
select count(1) from src_rc_merge_test_part where ds='2014-08-04';

set fs.hdfs.impl.disable.cache=true;
alter table src_rc_merge_test_part add partition (ds = '2014-08-05') location '${system:test.tmp.dir}/alter_merge_location/ds20140805';
alter table src_rc_merge_test_part partition (ds = '2014-08-05') concatenate;
select count(1) from src_rc_merge_test_part where ds='2014-08-05';

drop table src_rc_merge_test_part;