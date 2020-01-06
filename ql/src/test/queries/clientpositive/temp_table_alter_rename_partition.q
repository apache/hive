-- Cleanup
DROP TABLE alter_rename_partition_src_temp;
DROP TABLE alter_rename_partition_temp;

create temporary table alter_rename_partition_src_temp ( col1 string ) stored as textfile ;
load data local inpath '../../data/files/test.dat' overwrite into table alter_rename_partition_src_temp ;

create temporary table alter_rename_partition_temp ( col1 string ) partitioned by (pcol1 string , pcol2 string) stored as sequencefile;

insert overwrite table alter_rename_partition_temp partition (pCol1='old_part1:', pcol2='old_part2:') select col1 from alter_rename_partition_src_temp ;
select * from alter_rename_partition_temp where pcol1='old_part1:' and pcol2='old_part2:';

alter table alter_rename_partition_temp partition (pCol1='old_part1:', pcol2='old_part2:') rename to partition (pCol1='new_part1:', pcol2='new_part2:');
SHOW PARTITIONS alter_rename_partition_temp;
select * from alter_rename_partition_temp where pcol1='old_part1:' and pcol2='old_part2:';
select * from alter_rename_partition_temp where pcol1='new_part1:' and pcol2='new_part2:';

-- Cleanup
DROP TABLE alter_rename_partition_src_temp;
DROP TABLE alter_rename_partition_temp;

-- With non-default Database

CREATE DATABASE alter_rename_partition_db;
USE alter_rename_partition_db;
SHOW TABLES;

CREATE TEMPORARY TABLE alter_rename_partition_src_temp (col1 STRING) STORED AS TEXTFILE ;
LOAD DATA LOCAL INPATH '../../data/files/test.dat' OVERWRITE INTO TABLE alter_rename_partition_src_temp ;

CREATE TEMPORARY TABLE alter_rename_partition_temp (col1 STRING) PARTITIONED BY (pcol1 STRING, pcol2 STRING) STORED AS SEQUENCEFILE;

INSERT OVERWRITE TABLE alter_rename_partition_temp PARTITION (pCol1='old_part1:', pcol2='old_part2:') SELECT col1 FROM alter_rename_partition_src_temp ;
SELECT * FROM alter_rename_partition_temp WHERE pcol1='old_part1:' AND pcol2='old_part2:';

EXPLAIN ALTER TABLE alter_rename_partition_temp PARTITION (pCol1='old_part1:', pcol2='old_part2:') RENAME TO PARTITION (pCol1='new_part1:', pcol2='new_part2:');
ALTER TABLE alter_rename_partition_temp PARTITION (pCol1='old_part1:', pcol2='old_part2:') RENAME TO PARTITION (pCol1='new_part1:', pcol2='new_part2:');
SHOW PARTITIONS alter_rename_partition_temp;
SELECT * FROM alter_rename_partition_temp WHERE pcol1='old_part1:' and pcol2='old_part2:';
SELECT * FROM alter_rename_partition_temp WHERE pcol1='new_part1:' and pcol2='new_part2:';
