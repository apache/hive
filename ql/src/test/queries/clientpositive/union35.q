--! qt:dataset:src
-- SORT_QUERY_RESULTS

select * from (
     select * from ( select 1 as id , 'foo' as str_1 from src tablesample(5 rows)) f
 union all
     select * from ( select 2 as id , 'bar' as str_2 from src tablesample(5 rows)) g
) e ;

set hive.cbo.enable=false;

select * from (
     select * from ( select 1 as id , 'foo' as str_1 from src tablesample(5 rows)) f
 union all
     select * from ( select 2 as id , 'bar' as str_2 from src tablesample(5 rows)) g
) e ;
