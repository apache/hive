set hive.mapred.mode=nonstrict;
set datanucleus.cache.collections=false;
set hive.stats.autogather=true;
drop table hive_test_src_n3;
drop table hive_test_dst_n0;

create table hive_test_src_n3 ( col1 string ) stored as textfile ;
explain extended
load data local inpath '../../data/files/test.dat' overwrite into table hive_test_src_n3 ;

load data local inpath '../../data/files/test.dat' overwrite into table hive_test_src_n3 ;

desc formatted hive_test_src_n3;

create table hive_test_dst_n0 ( col1 string ) partitioned by ( pcol1 string , pcol2 string) stored as sequencefile;
insert overwrite table hive_test_dst_n0 partition ( pcol1='test_part', pCol2='test_Part') select col1 from hive_test_src_n3 ;
select * from hive_test_dst_n0 where pcol1='test_part' and pcol2='test_Part';

select count(1) from hive_test_dst_n0;

insert overwrite table hive_test_dst_n0 partition ( pCol1='test_part', pcol2='test_Part') select col1 from hive_test_src_n3 ;
select * from hive_test_dst_n0 where pcol1='test_part' and pcol2='test_part';

select count(1) from hive_test_dst_n0;

select * from hive_test_dst_n0 where pcol1='test_part';
select * from hive_test_dst_n0 where pcol1='test_part' and pcol2='test_part';
select * from hive_test_dst_n0 where pcol1='test_Part';

describe formatted hive_test_dst_n0;

drop table hive_test_src_n3;
drop table hive_test_dst_n0;
