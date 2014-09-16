SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET mapred.min.split.size=1000;
SET mapred.max.split.size=5000;

create table newtypesorc(c char(10), v varchar(10), d decimal(5,3), b boolean) stored as orc tblproperties("orc.stripe.size"="16777216"); 

insert overwrite table newtypesorc select * from (select cast("apple" as char(10)), cast("bee" as varchar(10)), 0.22, true from src src1 union all select cast("hello" as char(10)), cast("world" as varchar(10)), 11.22, false from src src2) uniontbl;

set hive.optimize.index.filter=false;

-- char data types (EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_EQUALS, IN, BETWEEN tests)
select sum(hash(*)) from newtypesorc where b=true;

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where b=false;

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where b!=true;

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where b!=false;

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where b<true;

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where b<false;

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc where b<=true;

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc where b<=false;

