-- EXCLUDE_OS_WINDOWS
-- excluded on windows because of difference in file name encoding logic

-- SORT_QUERY_RESULTS

create table if not exists nzhang_part14 (key string)
  partitioned by (value string);

describe extended nzhang_part14;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

explain
insert overwrite table nzhang_part14 partition(value) 
select key, value from (
  select * from (select 'k1' as key, cast(null as string) as value from src limit 2)a 
  union all
  select * from (select 'k2' as key, '' as value from src limit 2)b
  union all 
  select * from (select 'k3' as key, ' ' as value from src limit 2)c
) T;

insert overwrite table nzhang_part14 partition(value) 
select key, value from (
  select * from (select 'k1' as key, cast(null as string) as value from src limit 2)a 
  union all
  select * from (select 'k2' as key, '' as value from src limit 2)b
  union all 
  select * from (select 'k3' as key, ' ' as value from src limit 2)c
) T;


show partitions nzhang_part14;

select * from nzhang_part14 where value <> 'a';


