
create table over1k(
           t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           dec decimal(4,2),
           bin binary)
       row format delimited
       fields terminated by '|';

load data local inpath '../../data/files/over1k' into table over1k;

-- Pass non-strings for the first and third arguments to test argument conversion
-- For negative testing, try LIST, MAP, STRUCT, UNION

-- Integers
select lpad({"key1":[1,2,3],"key2":[6,7,8]}, 4, ' ') from over1k limit 5;