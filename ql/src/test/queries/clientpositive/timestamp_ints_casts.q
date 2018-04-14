--! qt:dataset:alltypesorc
set hive.int.timestamp.conversion.in.seconds=false;

explain
select
-- to timestamp
  cast (ctinyint as timestamp)
  ,cast (csmallint as timestamp)
  ,cast (cint as timestamp)
  ,cast (cbigint as timestamp)
  ,cast (cfloat as timestamp)
  ,cast (cdouble as timestamp)
  ,cast (cboolean1 as timestamp)
  ,cast (cbigint * 0 as timestamp)
  ,cast (ctimestamp1 as timestamp)
  ,cast (cstring1 as timestamp)
  ,cast (substr(cstring1, 1, 1) as timestamp)
from alltypesorc
-- limit output to a reasonably small number of rows
where cbigint % 250 = 0;

select
-- to timestamp
  cast (ctinyint as timestamp)
  ,cast (csmallint as timestamp)
  ,cast (cint as timestamp)
  ,cast (cbigint as timestamp)
  ,cast (cfloat as timestamp)
  ,cast (cdouble as timestamp)
  ,cast (cboolean1 as timestamp)
  ,cast (cbigint * 0 as timestamp)
  ,cast (ctimestamp1 as timestamp)
  ,cast (cstring1 as timestamp)
  ,cast (substr(cstring1, 1, 1) as timestamp)
from alltypesorc
-- limit output to a reasonably small number of rows
where cbigint % 250 = 0;

set hive.int.timestamp.conversion.in.seconds=true;

explain
select
-- to timestamp
  cast (ctinyint as timestamp)
  ,cast (csmallint as timestamp)
  ,cast (cint as timestamp)
  ,cast (cbigint as timestamp)
  ,cast (cfloat as timestamp)
  ,cast (cdouble as timestamp)
  ,cast (cboolean1 as timestamp)
  ,cast (cbigint * 0 as timestamp)
  ,cast (ctimestamp1 as timestamp)
  ,cast (cstring1 as timestamp)
  ,cast (substr(cstring1, 1, 1) as timestamp)
from alltypesorc
-- limit output to a reasonably small number of rows
where cbigint % 250 = 0;

select
-- to timestamp
  cast (ctinyint as timestamp)
  ,cast (csmallint as timestamp)
  ,cast (cint as timestamp)
  ,cast (cbigint as timestamp)
  ,cast (cfloat as timestamp)
  ,cast (cdouble as timestamp)
  ,cast (cboolean1 as timestamp)
  ,cast (cbigint * 0 as timestamp)
  ,cast (ctimestamp1 as timestamp)
  ,cast (cstring1 as timestamp)
  ,cast (substr(cstring1, 1, 1) as timestamp)
from alltypesorc
-- limit output to a reasonably small number of rows
where cbigint % 250 = 0;

