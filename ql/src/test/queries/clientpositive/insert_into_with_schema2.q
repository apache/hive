set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS;



create table studenttab10k (age2 int);
insert into studenttab10k values(1);

create table student_acid (age int, grade int)
 clustered by (age) into 1 buckets;

insert into student_acid(age) select * from studenttab10k;

select * from student_acid;

insert into student_acid(grade, age) select 3 g, * from studenttab10k;

select * from student_acid;

insert into student_acid(grade, age) values(20, 2);

insert into student_acid(age) values(22);

select * from student_acid;

set hive.exec.dynamic.partition.mode=nonstrict;

drop table if exists acid_partitioned;
create table acid_partitioned (a int, c string)
  partitioned by (p int)
  clustered by (a) into 1 buckets;

insert into acid_partitioned partition (p) (a,p) values(1,2);

select * from acid_partitioned;
