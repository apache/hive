


create temporary table hive_test_src_n2_temp ( col1 string ) stored as textfile ;
load data local inpath '../../data/files/test.dat' overwrite into table hive_test_src_n2_temp ;

create temporary table hive_test_dst_temp ( col1 string ) partitioned by ( pcol1 string , pcol2 string) stored as sequencefile;
insert overwrite table hive_test_dst_temp partition ( pcol1='test_part', pCol2='test_Part') select col1 from hive_test_src_n2_temp ;
select * from hive_test_dst_temp where pcol1='test_part' and pcol2='test_Part';

insert overwrite table hive_test_dst_temp partition ( pCol1='test_part', pcol2='test_Part') select col1 from hive_test_src_n2_temp ;
select * from hive_test_dst_temp where pcol1='test_part' and pcol2='test_part';

select * from hive_test_dst_temp where pcol1='test_part';
select * from hive_test_dst_temp where pcol1='test_part' and pcol2='test_part';
select * from hive_test_dst_temp where pcol1='test_Part';



