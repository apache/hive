create temporary table alter_rename_partition_src_temp ( col1 string ) stored as textfile ;
load data local inpath '../../data/files/test.dat' overwrite into table alter_rename_partition_src_temp ;
create temporary table alter_rename_partition_temp ( col1 string ) partitioned by (pcol1 string , pcol2 string) stored as sequencefile;
insert overwrite table alter_rename_partition_temp partition (pcol1='old_part1:', pcol2='old_part2:') select col1 from alter_rename_partition_src_temp ;
alter table alter_rename_partition_temp add partition(pcol1='new_part1:', pcol2='new_part2:');

alter table alter_rename_partition_temp partition (pcol1='old_part1:', pcol2='old_part2:') rename to partition (pcol1='new_part1:', pcol2='new_part2:');
