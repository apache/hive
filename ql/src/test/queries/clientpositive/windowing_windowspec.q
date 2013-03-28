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

load data local inpath '../data/files/over10k' into table over10k;

select s, sum(b) over (partition by i order by si rows unbounded preceding) from over10k limit 100;

select s, sum(f) over (partition by d order by i rows unbounded preceding) from over10k limit 100;

select s, sum(f) over (partition by ts order by b range between current row and unbounded following) from over10k limit 100;

select s, avg(f) over (partition by bin order by s rows between current row and 5 following) from over10k limit 100;

select s, avg(d) over (partition by t order by ts desc rows between 5 preceding and 5 following) from over10k limit 100;

select s, sum(i) over() from over10k limit 100;

select f, sum(f) over (order by f range between unbounded preceding and current row) from over10k limit 100;


