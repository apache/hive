
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

create temporary table emp1_temp (id int, name string, dept int, country string) row format delimited fields terminated by '|' stored as textfile;
load data local inpath '../../data/files/employee_part.txt' overwrite into table emp1_temp;
select * from emp1_temp order by id;

set hive.blobstore.supported.schemes=pfile;
-- Setting pfile to be treated as blobstore to test mvFileToFinalPath() behavior for blobstore case
-- inserts into non-partitioned/non-bucketed table
create temporary table emp2_temp (id int, name string, dept int, country string) stored as textfile;
insert overwrite table emp2_temp select * from emp1_temp;
select * from emp2_temp order by id;

-- inserts into partitioned/bucketed table
create temporary table emp1_temp_part_bucket_temp (id int, name string) partitioned by (dept int, country string) clustered by (id) into 4 buckets;
insert overwrite table emp1_temp_part_bucket_temp partition (dept, country) select * from emp1_temp;
show partitions emp1_temp_part_bucket_temp;
select * from emp1_temp_part_bucket_temp order by id;
