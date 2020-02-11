
create table tst_n1(a int, b int) partitioned by (d string);
alter table tst_n1 add partition (d='2009-01-01');
explain
select count(1) from tst_n1 x where x.d='2009-01-01';

select count(1) from tst_n1 x where x.d='2009-01-01';


