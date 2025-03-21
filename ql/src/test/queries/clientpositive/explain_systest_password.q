--! qt:sysdb

SET hive.fetch.task.conversion=none;

select 1
from sys.TBLS t
    join sys.DBS d on t.DB_ID = d.DB_ID
limit 1;

explain extended
select 1
from sys.TBLS t
    join sys.DBS d on t.DB_ID = d.DB_ID
limit 1;

show create table sys.DBS;

describe formatted sys.DBS;

create table if not exists ctas_dbs as select * from sys.DBS;

select 1
from sys.TBLS t
    join ctas_dbs d on t.DB_ID = d.DB_ID
limit 1;

explain extended
select 1
from sys.TBLS t
    join ctas_dbs d on t.DB_ID = d.DB_ID
limit 1;

create table if not exists ctlt_dbs like sys.DBS;

insert into ctlt_dbs
select * from sys.DBS;

select 1
from sys.TBLS t
    join ctlt_dbs d on t.DB_ID = d.DB_ID
limit 1;

explain extended
select 1
from sys.TBLS t
    join ctlt_dbs d on t.DB_ID = d.DB_ID
limit 1;
