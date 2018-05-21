SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;
set hive.mapred.mode=nonstrict;
set hive.stats.fetch.column.stats=true;

create table if not exists alltypes_n4 (
 bo1 boolean,
 ti1 tinyint,
 si1 smallint,
 i1 int,
 bi1 bigint,
 f1 float,
 d1 double,
 de1 decimal,
 ts1 timestamp,
 da1 timestamp,
 s1 string,
 vc1 varchar(5),
 m1 map<string, string>,
 l1 array<int>,
 st1 struct<c1:int, c2:string>
) row format delimited fields terminated by '|'
collection items terminated by ','
map keys terminated by ':' stored as textfile;

create table alltypes_orc_n4 like alltypes_n4;
alter table alltypes_orc_n4 set fileformat orc;

load data local inpath '../../data/files/alltypes.txt' overwrite into table alltypes_n4;

insert overwrite table alltypes_orc_n4 select * from alltypes_n4;

-- basicStatState: COMPLETE colStatState: NONE numRows: 2 rawDataSize: 1514
explain select * from alltypes_orc_n4;

-- statistics for complex types are not supported yet
analyze table alltypes_orc_n4 compute statistics for columns bo1, ti1, si1, i1, bi1, f1, d1, s1, vc1;

-- numRows: 2 rawDataSize: 1514
explain select * from alltypes_orc_n4;

-- numRows: 2 rawDataSize: 8
explain select bo1 from alltypes_orc_n4;

-- col alias renaming
-- numRows: 2 rawDataSize: 8
explain select i1 as int1 from alltypes_orc_n4;

-- numRows: 2 rawDataSize: 174
explain select s1 from alltypes_orc_n4;

-- column statistics for complex types unsupported and so statistics will not be updated
-- numRows: 2 rawDataSize: 1514
explain select m1 from alltypes_orc_n4;

-- numRows: 2 rawDataSize: 246
explain select bo1, ti1, si1, i1, bi1, f1, d1,s1 from alltypes_orc_n4;

-- numRows: 2 rawDataSize: 0
explain vectorization expression select null from alltypes_orc_n4;

-- numRows: 2 rawDataSize: 8
explain vectorization expression select 11 from alltypes_orc_n4;

-- numRows: 2 rawDataSize: 16
explain vectorization expression select 11L from alltypes_orc_n4;

-- numRows: 2 rawDataSize: 16
explain vectorization expression select 11.0 from alltypes_orc_n4;

-- numRows: 2 rawDataSize: 178
explain vectorization expression select "hello" from alltypes_orc_n4;
explain vectorization expression select cast("hello" as char(5)) from alltypes_orc_n4;
explain vectorization expression select cast("hello" as varchar(5)) from alltypes_orc_n4;

-- numRows: 2 rawDataSize: 96
explain vectorization expression select unbase64("0xe23") from alltypes_orc_n4;

-- numRows: 2 rawDataSize: 16
explain vectorization expression select cast("1" as TINYINT), cast("20" as SMALLINT) from alltypes_orc_n4;

-- numRows: 2 rawDataSize: 80
explain vectorization expression select cast("1970-12-31 15:59:58.174" as TIMESTAMP) from alltypes_orc_n4;

-- numRows: 2 rawDataSize: 112
explain vectorization expression select cast("1970-12-31 15:59:58.174" as DATE) from alltypes_orc_n4;

-- numRows: 2 rawDataSize: 224
explain vectorization expression select cast("58.174" as DECIMAL) from alltypes_orc_n4;

-- numRows: 2 rawDataSize: 112
explain vectorization expression select array(1,2,3) from alltypes_orc_n4;

-- numRows: 2 rawDataSize: 1508
explain vectorization expression select str_to_map("a=1 b=2 c=3", " ", "=") from alltypes_orc_n4;

-- numRows: 2 rawDataSize: 112
explain vectorization expression select NAMED_STRUCT("a", 11, "b", 11) from alltypes_orc_n4;

-- numRows: 2 rawDataSize: 250
explain vectorization expression select CREATE_UNION(0, "hello") from alltypes_orc_n4;

-- COUNT(*) is projected as new column. It is not projected as GenericUDF and so datasize estimate will be based on number of rows
-- numRows: 1 rawDataSize: 8
explain vectorization expression select count(*) from alltypes_orc_n4;

-- COUNT(1) is projected as new column. It is not projected as GenericUDF and so datasize estimate will be based on number of rows
-- numRows: 1 rawDataSize: 8
explain vectorization expression select count(1) from alltypes_orc_n4;

-- column statistics for complex column types will be missing. data size will be calculated from available column statistics
-- numRows: 2 rawDataSize: 254
explain vectorization expression select *,11 from alltypes_orc_n4;

-- subquery selects
-- inner select - numRows: 2 rawDataSize: 8
-- outer select - numRows: 2 rawDataSize: 8
explain vectorization expression select i1 from (select i1 from alltypes_orc_n4 limit 10) temp;

-- inner select - numRows: 2 rawDataSize: 16
-- outer select - numRows: 2 rawDataSize: 8
explain vectorization expression select i1 from (select i1,11 from alltypes_orc_n4 limit 10) temp;

-- inner select - numRows: 2 rawDataSize: 16
-- outer select - numRows: 2 rawDataSize: 186
explain vectorization expression select i1,"hello" from (select i1,11 from alltypes_orc_n4 limit 10) temp;

-- inner select - numRows: 2 rawDataSize: 24
-- outer select - numRows: 2 rawDataSize: 16
explain vectorization expression select x from (select i1,11.0 as x from alltypes_orc_n4 limit 10) temp;

-- inner select - numRows: 2 rawDataSize: 104
-- outer select - numRows: 2 rawDataSize: 186
explain vectorization expression select x,"hello" from (select i1 as x, unbase64("0xe23") as ub from alltypes_orc_n4 limit 10) temp;

-- inner select -  numRows: 2 rawDataSize: 186
-- middle select - numRows: 2 rawDataSize: 178
-- outer select -  numRows: 2 rawDataSize: 194
explain vectorization expression select h, 11.0 from (select hell as h from (select i1, "hello" as hell from alltypes_orc_n4 limit 10) in1 limit 10) in2;

-- This test is for FILTER operator where filter expression is a boolean column
-- numRows: 2 rawDataSize: 8
explain vectorization expression select bo1 from alltypes_orc_n4 where bo1;

-- numRows: 0 rawDataSize: 0
explain vectorization expression select bo1 from alltypes_orc_n4 where !bo1;
