--! qt:dataset:src1
--! qt:dataset:src

set hive.vectorized.execution.enabled=false;
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET mapred.min.split.size=1000;
SET mapred.max.split.size=5000;

create table newtypesorc(c char(10), v varchar(10), d date, ts timestamp) stored as orc tblproperties("orc.stripe.size"="16777216"); 

insert overwrite table newtypesorc select * from (select cast("apple" as char(10)), cast("bee" as varchar(10)), null, null from src src1 union all select cast("hello" as char(10)), cast("world" as varchar(10)), null, null from src src2) uniontbl;

select sum(hash(*)) from newtypesorc where ts is null;

select sum(hash(*)) from newtypesorc where ts is null;

select sum(hash(*)) from newtypesorc where d is null;

select sum(hash(*)) from newtypesorc where d is null;

