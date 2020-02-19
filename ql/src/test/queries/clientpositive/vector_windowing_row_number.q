set hive.cli.print.header=true;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.execution.ptf.enabled=true;
set hive.fetch.task.conversion=none;

drop table over10k_n8;

create table over10k_n8(
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

load data local inpath '../../data/files/over10k' into table over10k_n8;

select row_number() over() r1, t from over10k_n8 limit 1100;

select row_number() over(partition by 1) r1, t from over10k_n8 limit 1100;

select row_number() over(partition by 1,2,3,4,5) r1, t from over10k_n8 limit 1100;

select row_number() over() r1, row_number() over() r2, t from over10k_n8 limit 1100;

