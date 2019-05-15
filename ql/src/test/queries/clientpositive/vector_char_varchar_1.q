set hive.cli.print.header=true;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.vectorized.execution.enabled=true;

create table varchar_table(vs varchar(50));
insert into table varchar_table
    values ("blue"), ("a long sentence"), ("more sunshine and rain"), ("exactly 10"), ("longer 10 "), ("tells the truth"); 
alter table varchar_table change column vs vs varchar(10);


explain vectorization detail
create table varchar_ctas_1 as select length(vs),reverse(vs) from varchar_table;

create table varchar_ctas_1 as select length(vs),reverse(vs) from varchar_table;

select * from varchar_ctas_1 order by 2;


create table char_table(vs char(50));
insert into table char_table
    values ("  yellow"), ("some words and spaces  "), ("less sunshine and rain"), ("exactly 10"), ("sunlight moon"), ("begs the truth"); 
alter table char_table change column vs vs char(10);

explain vectorization detail
create table char_ctas_1 as select length(vs),reverse(vs) from char_table;

create table char_ctas_1 as select length(vs),reverse(vs) from char_table;

select * from char_ctas_1 order by 2;