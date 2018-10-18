
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

create table emp1 (id int, name string, dept int, country string) row format delimited fields terminated by '|' stored as textfile;
load data local inpath '../../data/files/employee_part.txt' overwrite into table emp1;
select * from emp1 order by id;

set hive.blobstore.supported.schemes=pfile;
-- Setting pfile to be treated as blobstore to test mvFileToFinalPath() behavior for blobstore case
-- inserts into non-partitioned/non-bucketed table
create table emp2 (id int, name string, dept int, country string) stored as textfile;
insert overwrite table emp2 select * from emp1;
select * from emp2 order by id;

-- inserts into partitioned/bucketed table
create table emp1_part_bucket (id int, name string) partitioned by (dept int, country string) clustered by (id) into 4 buckets;
insert overwrite table emp1_part_bucket partition (dept, country) select * from emp1;
show partitions emp1_part_bucket;
select * from emp1_part_bucket order by id;
