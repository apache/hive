drop table extract_udf;

create table extract_udf (t timestamp);
from (select * from src tablesample (1 rows)) s
  insert overwrite table extract_udf 
    select '2011-05-06 07:08:09.1234567';

select t
from extract_udf;

explain
select floor_day(t)
from extract_udf;

select floor_day(t)
from extract_udf;

-- new syntax
explain
select floor(t to day)
from extract_udf;

select floor(t to day)
from extract_udf;


select floor(t to second)
from extract_udf;

select floor(t to minute)
from extract_udf;

select floor(t to hour)
from extract_udf;

select floor(t to week)
from extract_udf;

select floor(t to month)
from extract_udf;

select floor(t to quarter)
from extract_udf;

select floor(t to year)
from extract_udf;
