--! qt:sysdb

select 1
from sys.TBLS t
    join sys.DBS d on t.DB_ID = d.DB_ID
limit 1;

explain extended
select 1
from sys.TBLS t
    join sys.DBS d on t.DB_ID = d.DB_ID
limit 1;
