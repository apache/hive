--! qt:dataset:src1
--! qt:dataset:src

set hive.vectorized.execution.enabled=false;
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.ppd=true;
SET mapred.min.split.size=1000;
SET mapred.max.split.size=5000;

create table newtypestbl_n3(c char(10), v varchar(10), d decimal(5,3), da date) stored as parquet;

insert overwrite table newtypestbl_n3 select * from (select cast("apple" as char(10)), cast("bee" as varchar(10)), 0.22, cast("1970-02-20" as date) from src src1 union all select cast("hello" as char(10)), cast("world" as varchar(10)), 11.22, cast("1970-02-27" as date) from src src2 limit 10) uniontbl;


-- char data types (EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_EQUALS, IN, BETWEEN tests)
select * from newtypestbl_n3 where c="apple";

select * from newtypestbl_n3 where c="apple";

select * from newtypestbl_n3 where c!="apple";

select * from newtypestbl_n3 where c!="apple";

select * from newtypestbl_n3 where c<"hello";

select * from newtypestbl_n3 where c<"hello";

select * from newtypestbl_n3 where c<="hello" sort by c;

select * from newtypestbl_n3 where c<="hello" sort by c;

select * from newtypestbl_n3 where c="apple ";

select * from newtypestbl_n3 where c="apple ";

select * from newtypestbl_n3 where c in ("apple", "carrot");

select * from newtypestbl_n3 where c in ("apple", "carrot");

select * from newtypestbl_n3 where c in ("apple", "hello") sort by c;

select * from newtypestbl_n3 where c in ("apple", "hello") sort by c;

select * from newtypestbl_n3 where c in ("carrot");

select * from newtypestbl_n3 where c in ("carrot");

select * from newtypestbl_n3 where c between "apple" and "carrot";

select * from newtypestbl_n3 where c between "apple" and "carrot";

select * from newtypestbl_n3 where c between "apple" and "zombie" sort by c;

select * from newtypestbl_n3 where c between "apple" and "zombie" sort by c;

select * from newtypestbl_n3 where c between "carrot" and "carrot1";

select * from newtypestbl_n3 where c between "carrot" and "carrot1";
