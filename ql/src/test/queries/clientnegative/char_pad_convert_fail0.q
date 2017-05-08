
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
-- Negative tests: LIST, MAP, STRUCT, UNION

-- Integers
select lpad(t, array(1,2,3), ' ') from over1k limit 5;