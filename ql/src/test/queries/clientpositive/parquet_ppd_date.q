SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.ppd=true;
SET mapred.min.split.size=1000;
SET mapred.max.split.size=5000;

create table newtypestbl(c char(10), v varchar(10), d decimal(5,3), da date) stored as parquet;

insert overwrite table newtypestbl select * from (select cast("apple" as char(10)), cast("bee" as varchar(10)), 0.22, cast("1970-02-20" as date) from src src1 union all select cast("hello" as char(10)), cast("world" as varchar(10)), 11.22, cast("1970-02-27" as date) from src src2 limit 10) uniontbl;

-- date data types (EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_EQUALS, IN, BETWEEN tests)
select * from newtypestbl where da='1970-02-20';

set hive.optimize.index.filter=true;
select * from newtypestbl where da='1970-02-20';

set hive.optimize.index.filter=true;
select * from newtypestbl where da= date '1970-02-20';

set hive.optimize.index.filter=false;
select * from newtypestbl where da=cast('1970-02-20' as date);

set hive.optimize.index.filter=true;
select * from newtypestbl where da=cast('1970-02-20' as date);

set hive.optimize.index.filter=false;
select * from newtypestbl where da=cast('1970-02-20' as varchar(20));

set hive.optimize.index.filter=true;
select * from newtypestbl where da=cast('1970-02-20' as varchar(20));

set hive.optimize.index.filter=false;
select * from newtypestbl where da!='1970-02-20';

set hive.optimize.index.filter=true;
select * from newtypestbl where da!='1970-02-20';

set hive.optimize.index.filter=false;
select * from newtypestbl where da<'1970-02-27';

set hive.optimize.index.filter=true;
select * from newtypestbl where da<'1970-02-27';

set hive.optimize.index.filter=false;
select * from newtypestbl where da<'1970-02-29' sort by c;

set hive.optimize.index.filter=true;
select * from newtypestbl where da<'1970-02-29' sort by c;

set hive.optimize.index.filter=false;
select * from newtypestbl where da<'1970-02-15';

set hive.optimize.index.filter=true;
select * from newtypestbl where da<'1970-02-15';

set hive.optimize.index.filter=false;
select * from newtypestbl where da<='1970-02-20';

set hive.optimize.index.filter=true;
select * from newtypestbl where da<='1970-02-20';

set hive.optimize.index.filter=false;
select * from newtypestbl where da<='1970-02-27' sort by c;

set hive.optimize.index.filter=true;
select * from newtypestbl where da<='1970-02-27' sort by c;

set hive.optimize.index.filter=false;
select * from newtypestbl where da in (cast('1970-02-21' as date), cast('1970-02-27' as date));

set hive.optimize.index.filter=true;
select * from newtypestbl where da in (cast('1970-02-21' as date), cast('1970-02-27' as date));

set hive.optimize.index.filter=false;
select * from newtypestbl where da in (cast('1970-02-20' as date), cast('1970-02-27' as date)) sort by c;

set hive.optimize.index.filter=true;
select * from newtypestbl where da in (cast('1970-02-20' as date), cast('1970-02-27' as date)) sort by c;

set hive.optimize.index.filter=false;
select * from newtypestbl where da in (cast('1970-02-21' as date), cast('1970-02-22' as date));

set hive.optimize.index.filter=true;
select * from newtypestbl where da in (cast('1970-02-21' as date), cast('1970-02-22' as date));

set hive.optimize.index.filter=false;
select * from newtypestbl where da between '1970-02-19' and '1970-02-22';

set hive.optimize.index.filter=true;
select * from newtypestbl where da between '1970-02-19' and '1970-02-22';

set hive.optimize.index.filter=false;
select * from newtypestbl where da between '1970-02-19' and '1970-02-28' sort by c;

set hive.optimize.index.filter=true;
select * from newtypestbl where da between '1970-02-19' and '1970-02-28' sort by c;

set hive.optimize.index.filter=false;
select * from newtypestbl where da between '1970-02-18' and '1970-02-19';

set hive.optimize.index.filter=true;
select * from newtypestbl where da between '1970-02-18' and '1970-02-19';
