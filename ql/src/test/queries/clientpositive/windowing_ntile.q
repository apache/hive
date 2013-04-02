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

select i, ntile(10) over (partition by s) from over10k limit 100;

select s, ntile(100) over (partition by i) from over10k limit 100;

select f, ntile(4) over (partition by d) from over10k limit 100;

select d, ntile(1000) over (partition by dec) from over10k limit 100;


