create table very_simple_table_for_in_test (name STRING);
insert into very_simple_table_for_in_test values ('a');

explain cbo
select * from very_simple_table_for_in_test where name IN('g','r') AND name IN('a','b') ;

select * from very_simple_table_for_in_test where name IN('g','r') AND name IN('a','b') ;
