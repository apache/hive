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

explain
select distinct(val1) from table_onestring;

-- long

explain vectorization detail
create table table_onebigint as select cbigint as val1 from alltypesorc;

create table table_onebigint as select cbigint as val1 from alltypesorc;

describe formatted table_onebigint;

describe formatted table_onebigint val1;

explain
select distinct(val1) from table_onebigint;

-- double

explain vectorization detail
create table table_onedouble as select cdouble as val1 from alltypesorc;

create table table_onedouble as select cdouble as val1 from alltypesorc;

describe formatted table_onedouble;

describe formatted table_onedouble val1;

explain
select distinct(val1) from table_onedouble;

-- timestamp

explain vectorization detail
create table table_onetimestamp as select ctimestamp1 as val1 from alltypesorc;

create table table_onetimestamp as select ctimestamp1 as val1 from alltypesorc;

describe formatted table_onetimestamp;

describe formatted table_onetimestamp val1;

explain
select distinct(val1) from table_onetimestamp;

--decimal

explain vectorization detail
create table table_onedecimal as select cast(cbigint as decimal(10,2)) as val1 from alltypesorc;

create table table_onedecimal as select cast(cbigint as decimal(10,2)) as val1 from alltypesorc;

describe formatted table_onedecimal;

describe formatted table_onedecimal val1;

explain
select distinct(val1) from table_onedecimal;


--decimal

explain vectorization detail
create table table_onedate as select cast(L_COMMITDATE as date) as val1 from lineitem;

create table table_onedate as select cast(L_COMMITDATE as date) as val1 from lineitem;

describe formatted table_onedate;

describe formatted table_onedate val1;

explain
select distinct(val1) from table_onedate;

