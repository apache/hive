SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET mapred.min.split.size=1000;
SET mapred.max.split.size=5000;

create table newtypestbl(c char(10), v varchar(10), d decimal(5,3), b boolean) stored as parquet;

insert overwrite table newtypestbl select * from (select cast("apple" as char(10)), cast("bee" as varchar(10)), 0.22, true from src src1 union all select cast("hello" as char(10)), cast("world" as varchar(10)), 11.22, false from src src2 limit 10) uniontbl;

SET hive.optimize.ppd=true;
SET hive.optimize.index.filter=true;
select * from newtypestbl where b=true;
select * from newtypestbl where b!=true;
select * from newtypestbl where b<true;
select * from newtypestbl where b>true;
select * from newtypestbl where b<=true sort by c;

select * from newtypestbl where b=false;
select * from newtypestbl where b!=false;
select * from newtypestbl where b<false;
select * from newtypestbl where b>false;
select * from newtypestbl where b<=false;


SET hive.optimize.index.filter=false;
select * from newtypestbl where b=true;
select * from newtypestbl where b!=true;
select * from newtypestbl where b<true;
select * from newtypestbl where b>true;
select * from newtypestbl where b<=true sort by c;

select * from newtypestbl where b=false;
select * from newtypestbl where b!=false;
select * from newtypestbl where b<false;
select * from newtypestbl where b>false;
select * from newtypestbl where b<=false;