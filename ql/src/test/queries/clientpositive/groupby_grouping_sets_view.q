-- SORT_QUERY_RESULTS

set hive.mapred.mode=nonstrict;

create database test;

create table test.case665558 (c1 string, c2 string);

insert into test.case665558 values ("1", "1");
insert into test.case665558 values ("2", "1");
insert into test.case665558 values ("3", "1");
insert into test.case665558 values ("1", "4");
insert into test.case665558 values ("1", "5");

create view   test.viewcase665558
as
select
   case
      when GROUPING__ID = 255 then `c1`
   end as `col_1`,
   case
      when GROUPING__ID = 255 then 3
   end as `col_2`,
   `c1`,
   `c2`
from
   `test`.`case665558`
group by
   `c1`,
   `c2`
GROUPING SETS
   (
      (`c1`),
      (`c1`, `c2`)
   );

select * from test.viewcase665558 ;


drop database test cascade;
