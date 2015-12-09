create table non_ascii_literal2 as
select "谢谢" as col1, "Абвгде" as col2;

select * from non_ascii_literal2
where col2 = "Абвгде";
