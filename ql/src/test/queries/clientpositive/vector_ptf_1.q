set hive.cli.print.header=true;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.vectorized.execution.enabled=true;

create table studentnull100(
name string,
age int,
gpa double)
row format delimited
fields terminated by '\001'
stored as textfile;
LOAD DATA LOCAL INPATH '../../data/files/studentnull100' OVERWRITE INTO TABLE studentnull100;
analyze table studentnull100 compute statistics;

explain vectorization detail
select age, name, avg(gpa), sum(age) over (partition by name)
from studentnull100
group by age, name;

select age, name, avg(gpa), sum(age) over (partition by name)
from studentnull100
group by age, name;