
create table over1k_n6(
           t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           `dec` decimal(4,2),
           bin binary)
       row format delimited
       fields terminated by '|';

load data local inpath '../../data/files/over1k' into table over1k_n6;

-- Pass non-strings for the first and third arguments to test argument conversion

-- Integers
select lpad(t, 4, ' '),
       lpad(si, 2, ' '),
       lpad(i, 9, 'z'),
       lpad(b, 2, 'a') from over1k_n6 limit 5;

select lpad("oh", 10, t),
       lpad("my", 6, si),
       lpad("other", 14, i),
       lpad("one", 12, b) from over1k_n6 limit 5;

-- Integers
select rpad(t, 4, ' '),
       rpad(si, 2, ' '),
       rpad(i, 9, 'z'),
       rpad(b, 2, 'a') from over1k_n6 limit 5;

select rpad("oh", 10, t),
       rpad("my", 6, si),
       rpad("other", 14, i),
       rpad("one", 12, b) from over1k_n6 limit 5;

-- More
select lpad(f, 4, ' '),
       lpad(d, 2, ' '),
       lpad(bo, 9, 'z'),
       lpad(ts, 2, 'a'),
       lpad(`dec`, 7, 'd'),
       lpad(bin, 8, 'b') from over1k_n6 limit 5;

select lpad("oh", 10, f),
       lpad("my", 6, d),
       lpad("other", 14, bo),
       lpad("one", 12, ts),
       lpad("two", 7, `dec`),
       lpad("three", 8, bin) from over1k_n6 limit 5;

select rpad(f, 4, ' '),
       rpad(d, 2, ' '),
       rpad(bo, 9, 'z'),
       rpad(ts, 2, 'a'),
       rpad(`dec`, 7, 'd'),
       rpad(bin, 8, 'b') from over1k_n6 limit 5;

select rpad("oh", 10, f),
       rpad("my", 6, d),
       rpad("other", 14, bo),
       rpad("one", 12, ts),
       rpad("two", 7, `dec`),
       rpad("three", 8, bin) from over1k_n6 limit 5;
