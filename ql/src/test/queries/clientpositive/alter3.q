drop table alter3_src;
drop table alter3;

create table alter3_src ( col1 string ) stored as textfile ;
load data local inpath '../data/files/test.dat' overwrite into table alter3_src ;

create table alter3 ( col1 string ) partitioned by (pcol1 string , pcol2 string) stored as sequencefile;

insert overwrite table alter3 partition (pCol1='test_part', pcol2='test_part') select col1 from alter3_src ;
select * from alter3 where pcol1='test_part' and pcol2='test_part';

alter table alter3 rename to alter3_renamed;
describe extended alter3_renamed;
describe extended alter3_renamed partition (pCol1='test_part', pcol2='test_part');
select * from alter3_renamed where pcol1='test_part' and pcol2='test_part';

drop table alter3_src;
drop table alter3;
drop table alter3_renamed;
