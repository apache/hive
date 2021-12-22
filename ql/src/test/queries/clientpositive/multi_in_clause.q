create table very_simple_table_for_in_test (name STRING, othername STRING);
insert into very_simple_table_for_in_test values ('a', null);
insert into very_simple_table_for_in_test values (null, null);

explain cbo
select * from very_simple_table_for_in_test where name IN('g','r') AND name IN('a','b') ;

select * from very_simple_table_for_in_test where name IN('g','r') AND name IN('a','b') ;

explain cbo
select name IN('g','r') AND name IN('a','b') from very_simple_table_for_in_test ;

select name IN('g','r') AND name IN('a','b') from very_simple_table_for_in_test ;

explain cbo
select name IN('g','r') AND name IN('a','b') AND othername IN('x', 'y') from very_simple_table_for_in_test ;

select name IN('g','r') AND name IN('a','b') AND othername IN('x', 'y') from very_simple_table_for_in_test ;
