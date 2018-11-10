--! qt:dataset:src1
--! qt:dataset:src

set hive.parquet.timestamp.skip.conversion=true;
set hive.vectorized.execution.enabled=false;
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.ppd=true;
SET mapred.min.split.size=1000;
SET mapred.max.split.size=5000;

create table newtypestbl_n3(c char(10), v varchar(10), d decimal(5,3), da date) stored as parquet;

insert overwrite table newtypestbl_n3 select * from (select cast("apple" as char(10)), cast("bee" as varchar(10)), 0.22, cast("1970-02-20" as date) from src src1 union all select cast("hello" as char(10)), cast("world" as varchar(10)), 11.22, cast("1970-02-27" as date) from src src2 limit 10) uniontbl;

set hive.optimize.index.filter=false;

-- char data types (EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_EQUALS, IN, BETWEEN tests)
select * from newtypestbl_n3 where c="apple";

set hive.optimize.index.filter=true;
select * from newtypestbl_n3 where c="apple";

set hive.optimize.index.filter=false;
select * from newtypestbl_n3 where c!="apple";

set hive.optimize.index.filter=true;
select * from newtypestbl_n3 where c!="apple";

set hive.optimize.index.filter=false;
select * from newtypestbl_n3 where c<"hello";

set hive.optimize.index.filter=true;
select * from newtypestbl_n3 where c<"hello";

set hive.optimize.index.filter=false;
select * from newtypestbl_n3 where c<="hello" sort by c;

set hive.optimize.index.filter=true;
select * from newtypestbl_n3 where c<="hello" sort by c;

set hive.optimize.index.filter=false;
select * from newtypestbl_n3 where c="apple ";

set hive.optimize.index.filter=true;
select * from newtypestbl_n3 where c="apple ";

set hive.optimize.index.filter=false;
select * from newtypestbl_n3 where c in ("apple", "carrot");

set hive.optimize.index.filter=true;
select * from newtypestbl_n3 where c in ("apple", "carrot");

set hive.optimize.index.filter=false;
select * from newtypestbl_n3 where c in ("apple", "hello") sort by c;

set hive.optimize.index.filter=true;
select * from newtypestbl_n3 where c in ("apple", "hello") sort by c;

set hive.optimize.index.filter=false;
select * from newtypestbl_n3 where c in ("carrot");

set hive.optimize.index.filter=true;
select * from newtypestbl_n3 where c in ("carrot");

set hive.optimize.index.filter=false;
select * from newtypestbl_n3 where c between "apple" and "carrot";

set hive.optimize.index.filter=true;
select * from newtypestbl_n3 where c between "apple" and "carrot";

set hive.optimize.index.filter=false;
select * from newtypestbl_n3 where c between "apple" and "zombie" sort by c;

set hive.optimize.index.filter=true;
select * from newtypestbl_n3 where c between "apple" and "zombie" sort by c;

set hive.optimize.index.filter=false;
select * from newtypestbl_n3 where c between "carrot" and "carrot1";

set hive.optimize.index.filter=true;
select * from newtypestbl_n3 where c between "carrot" and "carrot1";