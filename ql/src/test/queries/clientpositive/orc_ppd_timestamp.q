--! qt:dataset:src1
--! qt:dataset:src

set hive.vectorized.execution.enabled=false;
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET mapred.min.split.size=1000;
SET mapred.max.split.size=5000;

create table newtypesorc_n2(c char(10), v varchar(10), d decimal(5,3), ts timestamp) stored as orc tblproperties("orc.stripe.size"="16777216"); 

insert overwrite table newtypesorc_n2 select * from (select cast("apple" as char(10)), cast("bee" as varchar(10)), 0.22, cast("2011-01-01 01:01:01" as timestamp) from src src1 union all select cast("hello" as char(10)), cast("world" as varchar(10)), 11.22, cast("2011-01-20 01:01:01" as timestamp) from src src2) uniontbl;

-- timestamp data types (EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_EQUALS, IN, BETWEEN tests)
select sum(hash(*)) from newtypesorc_n2 where cast(ts as string)='2011-01-01 01:01:01';

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc_n2 where cast(ts as string)='2011-01-01 01:01:01';

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc_n2 where ts=cast('2011-01-01 01:01:01' as timestamp);

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc_n2 where ts=cast('2011-01-01 01:01:01' as timestamp);

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc_n2 where ts=cast('2011-01-01 01:01:01' as varchar(20));

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc_n2 where ts=cast('2011-01-01 01:01:01' as varchar(20));

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc_n2 where ts!=cast('2011-01-01 01:01:01' as timestamp);

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc_n2 where ts!=cast('2011-01-01 01:01:01' as timestamp);

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc_n2 where ts<cast('2011-01-20 01:01:01' as timestamp);

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc_n2 where ts<cast('2011-01-20 01:01:01' as timestamp);

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc_n2 where ts<cast('2011-01-22 01:01:01' as timestamp);

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc_n2 where ts<cast('2011-01-22 01:01:01' as timestamp);

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc_n2 where ts<cast('2010-10-01 01:01:01' as timestamp);

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc_n2 where ts<cast('2010-10-01 01:01:01' as timestamp);

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc_n2 where ts<=cast('2011-01-01 01:01:01' as timestamp);

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc_n2 where ts<=cast('2011-01-01 01:01:01' as timestamp);

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc_n2 where ts<=cast('2011-01-20 01:01:01' as timestamp);

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc_n2 where ts<=cast('2011-01-20 01:01:01' as timestamp);

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc_n2 where ts in (cast('2011-01-02 01:01:01' as timestamp), cast('2011-01-20 01:01:01' as timestamp));

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc_n2 where ts in (cast('2011-01-02 01:01:01' as timestamp), cast('2011-01-20 01:01:01' as timestamp));

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc_n2 where ts in (cast('2011-01-01 01:01:01' as timestamp), cast('2011-01-20 01:01:01' as timestamp));

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc_n2 where ts in (cast('2011-01-01 01:01:01' as timestamp), cast('2011-01-20 01:01:01' as timestamp));

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc_n2 where ts in (cast('2011-01-02 01:01:01' as timestamp), cast('2011-01-08 01:01:01' as timestamp));

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc_n2 where ts in (cast('2011-01-02 01:01:01' as timestamp), cast('2011-01-08 01:01:01' as timestamp));

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc_n2 where ts between cast('2010-10-01 01:01:01' as timestamp) and cast('2011-01-08 01:01:01' as timestamp);

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc_n2 where ts between cast('2010-10-01 01:01:01' as timestamp) and cast('2011-01-08 01:01:01' as timestamp);

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc_n2 where ts between cast('2010-10-01 01:01:01' as timestamp) and cast('2011-01-25 01:01:01' as timestamp);

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc_n2 where ts between cast('2010-10-01 01:01:01' as timestamp) and cast('2011-01-25 01:01:01' as timestamp);

set hive.optimize.index.filter=false;
select sum(hash(*)) from newtypesorc_n2 where ts between cast('2010-10-01 01:01:01' as timestamp) and cast('2010-11-01 01:01:01' as timestamp);

set hive.optimize.index.filter=true;
select sum(hash(*)) from newtypesorc_n2 where ts between cast('2010-10-01 01:01:01' as timestamp) and cast('2010-11-01 01:01:01' as timestamp);
