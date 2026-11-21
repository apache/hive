--! qt:dataset:src1
--! qt:dataset:src

set hive.vectorized.execution.enabled=false;
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET mapred.min.split.size=1000;
SET mapred.max.split.size=5000;

create table newtypesorc_n1(c char(10), v varchar(10), d decimal(5,3), da date) stored as orc tblproperties("orc.stripe.size"="16777216"); 

insert overwrite table newtypesorc_n1 select * from (select cast("apple" as char(10)), cast("bee" as varchar(10)), 0.22, cast("1970-02-20" as date) from src src1 union all select cast("hello" as char(10)), cast("world" as varchar(10)), 11.22, cast("1970-02-27" as date) from src src2) uniontbl;


-- varchar data types (EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_EQUALS, IN, BETWEEN tests)
select sum(hash(*)) from newtypesorc_n1 where v="bee";

select sum(hash(*)) from newtypesorc_n1 where v="bee";

select sum(hash(*)) from newtypesorc_n1 where v!="bee";

select sum(hash(*)) from newtypesorc_n1 where v!="bee";

select sum(hash(*)) from newtypesorc_n1 where v<"world";

select sum(hash(*)) from newtypesorc_n1 where v<"world";

select sum(hash(*)) from newtypesorc_n1 where v<="world";

select sum(hash(*)) from newtypesorc_n1 where v<="world";

select sum(hash(*)) from newtypesorc_n1 where v="bee   ";

select sum(hash(*)) from newtypesorc_n1 where v="bee   ";

select sum(hash(*)) from newtypesorc_n1 where v in ("bee", "orange");

select sum(hash(*)) from newtypesorc_n1 where v in ("bee", "orange");

select sum(hash(*)) from newtypesorc_n1 where v in ("bee", "world");

select sum(hash(*)) from newtypesorc_n1 where v in ("bee", "world");

select sum(hash(*)) from newtypesorc_n1 where v in ("orange");

select sum(hash(*)) from newtypesorc_n1 where v in ("orange");

select sum(hash(*)) from newtypesorc_n1 where v between "bee" and "orange";

select sum(hash(*)) from newtypesorc_n1 where v between "bee" and "orange";

select sum(hash(*)) from newtypesorc_n1 where v between "bee" and "zombie";

select sum(hash(*)) from newtypesorc_n1 where v between "bee" and "zombie";

select sum(hash(*)) from newtypesorc_n1 where v between "orange" and "pine";

select sum(hash(*)) from newtypesorc_n1 where v between "orange" and "pine";

