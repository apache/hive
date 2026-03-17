set hive.vectorized.execution.enabled=true;
set hive.cbo.enable=true;
set hive.auto.convert.join=true;

CREATE EXTERNAL TABLE main_tbl(col1 string, col2 string) stored as orc;
CREATE EXTERNAL TABLE sub_tbl(pdate date) stored as orc;
insert into main_tbl values('20250331','BBB'),('20250331','AAAAAA');
insert into sub_tbl values('2025-03-31');

--selectExpressions: 
--  IfExprColumnCondExpr(col 12:boolean, col 4:string, col 5:string)(
--    children: 
--      StringGroupColEqualStringScalar(col 5: string, val AAAAAA)(
--        children: 
--          StringUpper(col 4:string)(
--            children: 
--              StringTrimCol(col 1:string) -> 4:string
--          ) -> 5:string
--      ) -> 12:boolean, 
--      
--      ConstantVectorExpression(val AAAA_BBBB_CCCC_DDDD) -> 4:string, 
--      
--      IfExprStringScalarStringScalar(col 13:boolean, val WWWW_XXXX_YYYY_ZZZZ, val N/A)(
--        children: 
--          StringGroupColEqualStringScalar(col 6:string, val BBB)(
--            children: 
--              StringUpper(col 5:string)(
--                children: 
--                  StringTrimCol(col 1:string) -> 5:string
--              ) -> 6:string
--          ) -> 13:boolean
--      ) -> 5:string
--  ) -> 6:string

-- In the above expression, we can see that col 6, which is the final output column, is also used by a child 
-- expression to store the result of StringUpper. This may cause issues if we do not evaluate the child expression first.


explain vectorization detail
select case
    when upper(trim(col2)) = 'AAAAAA' then 'AAAA_BBBB_CCCC_DDDD'
    when upper(trim(col2)) = 'BBB' then 'WWWW_XXXX_YYYY_ZZZZ'
    else 'N/A'
end as result
from main_tbl
where cast(concat(substr(trim(col1),1,4),'-',substr(trim(col1),5,2),'-',substr(trim(col1),7,2)) as date) in (select pdate from sub_tbl);

select case
    when upper(trim(col2)) = 'AAAAAA' then 'AAAA_BBBB_CCCC_DDDD'
    when upper(trim(col2)) = 'BBB' then 'WWWW_XXXX_YYYY_ZZZZ'
    else 'N/A'
end as result
from main_tbl
where cast(concat(substr(trim(col1),1,4),'-',substr(trim(col1),5,2),'-',substr(trim(col1),7,2)) as date) in (select pdate from sub_tbl);
