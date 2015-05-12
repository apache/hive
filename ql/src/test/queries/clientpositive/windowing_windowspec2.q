drop table over10k;

create table over10k(
           t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
	   ts timestamp,
           dec decimal,
           bin binary)
       row format delimited
       fields terminated by '|';

load data local inpath '../../data/files/over10k' into table over10k;

select ts, f, sum(f) over (partition by ts order by f rows between 2 preceding and 1 preceding) from over10k limit 100;
select ts, f, sum(f) over (partition by ts order by f rows between unbounded preceding and 1 preceding) from over10k limit 100;
select ts, f, sum(f) over (partition by ts order by f rows between 1 following and 2 following) from over10k limit 100;
select ts, f, sum(f) over (partition by ts order by f rows between unbounded preceding and 1 following) from over10k limit 100;

