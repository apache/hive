-- SORT_QUERY_RESULTS

create table facts (val string);

insert into facts values ('abcdefghijklmnopqrstuvwxyz0123456789');

set hive.vectorized.execution.enabled=false;
drop table groupingsets32;
drop table groupingsets33;
drop table groupingsets32a;
drop table groupingsets33a;

create table groupingsets32 as 
select 
c00,c01,c02,c03,c04,c05,c06,c07,c08,c09, 
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19, 
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29, 
c30,c31 
,count(*) as n from ( 
select 
substring(val,01,1) as c00, substring(val,02,1) as c01, substring(val,03,1) as c02,substring(val,04,1) as c03,substring(val,05,1) as c04,substring(val,06,1) as c05,substring(val,07,1) as c06, substring(val,08,1) as c07,substring(val,09,1) as c08,substring(val,10,1) as c09, 
substring(val,11,1) as c10, substring(val,12,1) as c11, substring(val,13,1) as c12,substring(val,14,1) as c13,substring(val,15,1) as c14,substring(val,16,1) as c15,substring(val,17,1) as c16, substring(val,18,1) as c17,substring(val,19,1) as c18,substring(val,20,1) as c19, 
substring(val,21,1) as c20, substring(val,22,1) as c21, substring(val,23,1) as c22,substring(val,24,1) as c23,substring(val,25,1) as c24,substring(val,26,1) as c25,substring(val,27,1) as c26, substring(val,28,1) as c27,substring(val,29,1) as c28,substring(val,30,1) as c29, 
substring(val,31,1) as c30,substring(val,32,1) as c31 
from facts ) x 
group by 
c00,c01,c02,c03,c04,c05,c06,c07,c08,c09, 
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19, 
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29, 
c30,c31 
grouping sets ( 
c00,c01,c02,c03,c04,c05,c06,c07,c08,c09, 
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19, 
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29, 
c30,c31 
);

select * from groupingsets32;

create table groupingsets32a as
select
c00,c01,c02,c03,c04,c05,c06,c07,c08,c09,
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,
c30,c31,
count(*) as n,
grouping__id,
grouping(c00,c01,c02,c03,c04,c05,c06,c07,c08,c09,
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,
c30,c31)
from (
select
substring(val,01,1) as c00, substring(val,02,1) as c01, substring(val,03,1) as c02,substring(val,04,1) as c03,substring(val,05,1) as c04,substring(val,06,1) as c05,substring(val,07,1) as c06, substring(val,08,1) as c07,substring(val,09,1) as c08,substring(val,10,1) as c09,
substring(val,11,1) as c10, substring(val,12,1) as c11, substring(val,13,1) as c12,substring(val,14,1) as c13,substring(val,15,1) as c14,substring(val,16,1) as c15,substring(val,17,1) as c16, substring(val,18,1) as c17,substring(val,19,1) as c18,substring(val,20,1) as c19,
substring(val,21,1) as c20, substring(val,22,1) as c21, substring(val,23,1) as c22,substring(val,24,1) as c23,substring(val,25,1) as c24,substring(val,26,1) as c25,substring(val,27,1) as c26, substring(val,28,1) as c27,substring(val,29,1) as c28,substring(val,30,1) as c29,
substring(val,31,1) as c30,substring(val,32,1) as c31
from facts ) x
group by
c00,c01,c02,c03,c04,c05,c06,c07,c08,c09,
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,
c30,c31
grouping sets (
c00,c01,c02,c03,c04,c05,c06,c07,c08,c09,
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,
c30,c31
);

select * from groupingsets32a;

create table groupingsets33 as 
select 
c00,c01,c02,c03,c04,c05,c06,c07,c08,c09, 
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19, 
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29, 
c30,c31,c32 
,count(*) as n from ( 
select 
substring(val,01,1) as c00, substring(val,02,1) as c01, substring(val,03,1) as c02,substring(val,04,1) as c03,substring(val,05,1) as c04,substring(val,06,1) as c05,substring(val,07,1) as c06, substring(val,08,1) as c07,substring(val,09,1) as c08,substring(val,10,1) as c09, 
substring(val,11,1) as c10, substring(val,12,1) as c11, substring(val,13,1) as c12,substring(val,14,1) as c13,substring(val,15,1) as c14,substring(val,16,1) as c15,substring(val,17,1) as c16, substring(val,18,1) as c17,substring(val,19,1) as c18,substring(val,20,1) as c19, 
substring(val,21,1) as c20, substring(val,22,1) as c21, substring(val,23,1) as c22,substring(val,24,1) as c23,substring(val,25,1) as c24,substring(val,26,1) as c25,substring(val,27,1) as c26, substring(val,28,1) as c27,substring(val,29,1) as c28,substring(val,30,1) as c29, 
substring(val,31,1) as c30,substring(val,32,1) as c31,substring(val,33,1) as c32 
from facts ) x 
group by 
c00,c01,c02,c03,c04,c05,c06,c07,c08,c09, 
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19, 
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29, 
c30,c31,c32 
grouping sets ( 
c00,c01,c02,c03,c04,c05,c06,c07,c08,c09, 
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19, 
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29, 
c30,c31,c32 
) ;

select * from groupingsets33; 

create table groupingsets33a as
select
c00,c01,c02,c03,c04,c05,c06,c07,c08,c09,
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,
c30,c31,c32
,count(*) as n,
grouping__id,
grouping(c00,c01,c02,c03,c04,c05,c06,c07,c08,c09,
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,
c30,c31,c32) 
from (
select
substring(val,01,1) as c00, substring(val,02,1) as c01, substring(val,03,1) as c02,substring(val,04,1) as c03,substring(val,05,1) as c04,substring(val,06,1) as c05,substring(val,07,1) as c06, substring(val,08,1) as c07,substring(val,09,1) as c08,substring(val,10,1) as c09,
substring(val,11,1) as c10, substring(val,12,1) as c11, substring(val,13,1) as c12,substring(val,14,1) as c13,substring(val,15,1) as c14,substring(val,16,1) as c15,substring(val,17,1) as c16, substring(val,18,1) as c17,substring(val,19,1) as c18,substring(val,20,1) as c19,
substring(val,21,1) as c20, substring(val,22,1) as c21, substring(val,23,1) as c22,substring(val,24,1) as c23,substring(val,25,1) as c24,substring(val,26,1) as c25,substring(val,27,1) as c26, substring(val,28,1) as c27,substring(val,29,1) as c28,substring(val,30,1) as c29,
substring(val,31,1) as c30,substring(val,32,1) as c31,substring(val,33,1) as c32
from facts ) x
group by
c00,c01,c02,c03,c04,c05,c06,c07,c08,c09,
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,
c30,c31,c32
grouping sets (
c00,c01,c02,c03,c04,c05,c06,c07,c08,c09,
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,
c30,c31,c32
) ;

select * from groupingsets33a;

set hive.vectorized.execution.enabled=true;

drop table groupingsets32;
drop table groupingsets33;

drop table groupingsets32;
drop table groupingsets33;
drop table groupingsets32a;
drop table groupingsets33a;

create table groupingsets32 as 
select 
c00,c01,c02,c03,c04,c05,c06,c07,c08,c09, 
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19, 
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29, 
c30,c31 
,count(*) as n from ( 
select 
substring(val,01,1) as c00, substring(val,02,1) as c01, substring(val,03,1) as c02,substring(val,04,1) as c03,substring(val,05,1) as c04,substring(val,06,1) as c05,substring(val,07,1) as c06, substring(val,08,1) as c07,substring(val,09,1) as c08,substring(val,10,1) as c09, 
substring(val,11,1) as c10, substring(val,12,1) as c11, substring(val,13,1) as c12,substring(val,14,1) as c13,substring(val,15,1) as c14,substring(val,16,1) as c15,substring(val,17,1) as c16, substring(val,18,1) as c17,substring(val,19,1) as c18,substring(val,20,1) as c19, 
substring(val,21,1) as c20, substring(val,22,1) as c21, substring(val,23,1) as c22,substring(val,24,1) as c23,substring(val,25,1) as c24,substring(val,26,1) as c25,substring(val,27,1) as c26, substring(val,28,1) as c27,substring(val,29,1) as c28,substring(val,30,1) as c29, 
substring(val,31,1) as c30,substring(val,32,1) as c31 
from facts ) x 
group by 
c00,c01,c02,c03,c04,c05,c06,c07,c08,c09, 
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19, 
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29, 
c30,c31 
grouping sets ( 
c00,c01,c02,c03,c04,c05,c06,c07,c08,c09, 
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19, 
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29, 
c30,c31 
);

select * from groupingsets32;

create table groupingsets32a as
select
c00,c01,c02,c03,c04,c05,c06,c07,c08,c09,
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,
c30,c31,
count(*) as n,
grouping__id,
grouping(c00,c01,c02,c03,c04,c05,c06,c07,c08,c09,
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,
c30,c31)
from (
select
substring(val,01,1) as c00, substring(val,02,1) as c01, substring(val,03,1) as c02,substring(val,04,1) as c03,substring(val,05,1) as c04,substring(val,06,1) as c05,substring(val,07,1) as c06, substring(val,08,1) as c07,substring(val,09,1) as c08,substring(val,10,1) as c09,
substring(val,11,1) as c10, substring(val,12,1) as c11, substring(val,13,1) as c12,substring(val,14,1) as c13,substring(val,15,1) as c14,substring(val,16,1) as c15,substring(val,17,1) as c16, substring(val,18,1) as c17,substring(val,19,1) as c18,substring(val,20,1) as c19,
substring(val,21,1) as c20, substring(val,22,1) as c21, substring(val,23,1) as c22,substring(val,24,1) as c23,substring(val,25,1) as c24,substring(val,26,1) as c25,substring(val,27,1) as c26, substring(val,28,1) as c27,substring(val,29,1) as c28,substring(val,30,1) as c29,
substring(val,31,1) as c30,substring(val,32,1) as c31
from facts ) x
group by
c00,c01,c02,c03,c04,c05,c06,c07,c08,c09,
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,
c30,c31
grouping sets (
c00,c01,c02,c03,c04,c05,c06,c07,c08,c09,
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,
c30,c31
);

select * from groupingsets32a;

create table groupingsets33 as 
select 
c00,c01,c02,c03,c04,c05,c06,c07,c08,c09, 
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19, 
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29, 
c30,c31,c32 
,count(*) as n from ( 
select 
substring(val,01,1) as c00, substring(val,02,1) as c01, substring(val,03,1) as c02,substring(val,04,1) as c03,substring(val,05,1) as c04,substring(val,06,1) as c05,substring(val,07,1) as c06, substring(val,08,1) as c07,substring(val,09,1) as c08,substring(val,10,1) as c09, 
substring(val,11,1) as c10, substring(val,12,1) as c11, substring(val,13,1) as c12,substring(val,14,1) as c13,substring(val,15,1) as c14,substring(val,16,1) as c15,substring(val,17,1) as c16, substring(val,18,1) as c17,substring(val,19,1) as c18,substring(val,20,1) as c19, 
substring(val,21,1) as c20, substring(val,22,1) as c21, substring(val,23,1) as c22,substring(val,24,1) as c23,substring(val,25,1) as c24,substring(val,26,1) as c25,substring(val,27,1) as c26, substring(val,28,1) as c27,substring(val,29,1) as c28,substring(val,30,1) as c29, 
substring(val,31,1) as c30,substring(val,32,1) as c31,substring(val,33,1) as c32 
from facts ) x 
group by 
c00,c01,c02,c03,c04,c05,c06,c07,c08,c09, 
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19, 
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29, 
c30,c31,c32 
grouping sets ( 
c00,c01,c02,c03,c04,c05,c06,c07,c08,c09, 
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19, 
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29, 
c30,c31,c32 
) ;

select * from groupingsets33; 

create table groupingsets33a as
select
c00,c01,c02,c03,c04,c05,c06,c07,c08,c09,
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,
c30,c31,c32
,count(*) as n,
grouping__id,
grouping(c00,c01,c02,c03,c04,c05,c06,c07,c08,c09,
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,
c30,c31,c32) 
from (
select
substring(val,01,1) as c00, substring(val,02,1) as c01, substring(val,03,1) as c02,substring(val,04,1) as c03,substring(val,05,1) as c04,substring(val,06,1) as c05,substring(val,07,1) as c06, substring(val,08,1) as c07,substring(val,09,1) as c08,substring(val,10,1) as c09,
substring(val,11,1) as c10, substring(val,12,1) as c11, substring(val,13,1) as c12,substring(val,14,1) as c13,substring(val,15,1) as c14,substring(val,16,1) as c15,substring(val,17,1) as c16, substring(val,18,1) as c17,substring(val,19,1) as c18,substring(val,20,1) as c19,
substring(val,21,1) as c20, substring(val,22,1) as c21, substring(val,23,1) as c22,substring(val,24,1) as c23,substring(val,25,1) as c24,substring(val,26,1) as c25,substring(val,27,1) as c26, substring(val,28,1) as c27,substring(val,29,1) as c28,substring(val,30,1) as c29,
substring(val,31,1) as c30,substring(val,32,1) as c31,substring(val,33,1) as c32
from facts ) x
group by
c00,c01,c02,c03,c04,c05,c06,c07,c08,c09,
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,
c30,c31,c32
grouping sets (
c00,c01,c02,c03,c04,c05,c06,c07,c08,c09,
c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,
c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,
c30,c31,c32
) ;

select * from groupingsets33a;
