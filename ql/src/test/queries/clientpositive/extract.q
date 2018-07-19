--! qt:dataset:src
drop table extract_udf;

create table extract_udf (t timestamp);
from (select * from src tablesample (1 rows)) s
  insert overwrite table extract_udf 
    select '2011-05-06 07:08:09.1234567';

explain
select day(t)
from extract_udf;

select day(t)
from extract_udf;

-- new syntax
explain
select extract(day from t)
from extract_udf;

select extract(day from t)
from extract_udf;


select extract(second from t)
from extract_udf;

select extract(minute from t)
from extract_udf;

select extract(hour from t)
from extract_udf;

select extract(dayofweek from t)
from extract_udf;

select extract(week from t)
from extract_udf;

select extract(month from t)
from extract_udf;

select extract(quarter from t)
from extract_udf;

select extract(year from t)
from extract_udf;
