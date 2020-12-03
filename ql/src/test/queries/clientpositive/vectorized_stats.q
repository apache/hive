--! qt:dataset:alltypesorc
--! qt:dataset:lineitem

set hive.stats.autogather=true;
set hive.explain.user=false;
set hive.vectorized.execution.enabled=true;

-- string

explain vectorization detail
create table table_onestring as select cstring1 as val1 from alltypesorc;

create table table_onestring as select cstring1 as val1 from alltypesorc;

describe formatted table_onestring;

describe formatted table_onestring val1;

select distinct(val1) from table_onestring limit 10;

select distinct(cstring1) from alltypesorc limit 10;

-- long

explain vectorization detail
create table table_onebigint as select cbigint as val1 from alltypesorc;

create table table_onebigint as select cbigint as val1 from alltypesorc;

describe formatted table_onebigint;

describe formatted table_onebigint val1;

select distinct(val1) from table_onebigint limit 10;

select distinct(cbigint) from alltypesorc limit 10;

-- double

explain vectorization detail
create table table_onedouble as select cdouble as val1 from alltypesorc;

create table table_onedouble as select cdouble as val1 from alltypesorc;

describe formatted table_onedouble;

describe formatted table_onedouble val1;

select distinct(val1) from table_onedouble limit 10;

select distinct(cdouble) from alltypesorc limit 10;

-- timestamp

explain vectorization detail
create table table_onetimestamp as select ctimestamp1 as val1 from alltypesorc;

create table table_onetimestamp as select ctimestamp1 as val1 from alltypesorc;

describe formatted table_onetimestamp;

describe formatted table_onetimestamp val1;

select distinct(val1) from table_onetimestamp limit 10;

select distinct(ctimestamp1) from alltypesorc limit 10;

--decimal

explain vectorization detail
create table table_onedecimal as select cast(cbigint as decimal(10,2)) as val1 from alltypesorc;

create table table_onedecimal as select cast(cbigint as decimal(10,2)) as val1 from alltypesorc;

describe formatted table_onedecimal;

describe formatted table_onedecimal val1;

select distinct(val1) from table_onedecimal limit 10;

select distinct(cast(cbigint as decimal(10,2))) from alltypesorc limit 10;


--decimal

explain vectorization detail
create table table_onedate as select cast(L_COMMITDATE as date) as val1 from lineitem;

create table table_onedate as select cast(L_COMMITDATE as date) as val1 from lineitem;

describe formatted table_onedate;

describe formatted table_onedate val1;

select distinct(val1) from table_onedate limit 10;

select distinct(cast(L_COMMITDATE as date)) from lineitem limit 10;

