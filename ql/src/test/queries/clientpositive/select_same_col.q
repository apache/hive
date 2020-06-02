--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.cbo.enable=true;

-- SORT_QUERY_RESULTS

drop table srclimit;
create table srclimit as select * from src limit 10;

select cast(value as binary), value from srclimit;

select cast(value as binary), value from srclimit order by value;

select cast(value as binary), value from srclimit order by value limit 5;

select cast(value as binary), value, key from srclimit order by value, key limit 5;

select *, key, value from srclimit;

select * from (select *, key, value from srclimit) t;

drop table srclimit;
