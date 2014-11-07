set hive.cbo.enable=true;
set hive.exec.check.crossproducts=false;

set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join=false;

-- 9. Test Windowing Functions
select count(c_int) over() from cbo_t1;
select count(c_int) over(), sum(c_float) over(), max(c_int) over(), min(c_int) over(), row_number() over(), rank() over(), dense_rank() over(), percent_rank() over(), lead(c_int, 2, c_int) over(), lag(c_float, 2, c_float) over() from cbo_t1;
select * from (select count(c_int) over(), sum(c_float) over(), max(c_int) over(), min(c_int) over(), row_number() over(), rank() over(), dense_rank() over(), percent_rank() over(), lead(c_int, 2, c_int) over(), lag(c_float, 2, c_float) over() from cbo_t1) cbo_t1;
select x from (select count(c_int) over() as x, sum(c_float) over() from cbo_t1) cbo_t1;
select 1+sum(c_int) over() from cbo_t1;
select sum(c_int)+sum(sum(c_int)) over() from cbo_t1;
select * from (select max(c_int) over (partition by key order by value Rows UNBOUNDED PRECEDING), min(c_int) over (partition by key order by value rows current row), count(c_int) over(partition by key order by value ROWS 1 PRECEDING), avg(value) over (partition by key order by value Rows between unbounded preceding and unbounded following), sum(value) over (partition by key order by value rows between unbounded preceding and current row), avg(c_float) over (partition by key order by value Rows between 1 preceding and unbounded following), sum(c_float) over (partition by key order by value rows between 1 preceding and current row), max(c_float) over (partition by key order by value rows between 1 preceding and unbounded following), min(c_float) over (partition by key order by value rows between 1 preceding and 1 following) from cbo_t1) cbo_t1;
select i, a, h, b, c, d, e, f, g, a as x, a +1 as y from (select max(c_int) over (partition by key order by value range UNBOUNDED PRECEDING) a, min(c_int) over (partition by key order by value range current row) b, count(c_int) over(partition by key order by value range 1 PRECEDING) c, avg(value) over (partition by key order by value range between unbounded preceding and unbounded following) d, sum(value) over (partition by key order by value range between unbounded preceding and current row) e, avg(c_float) over (partition by key order by value range between 1 preceding and unbounded following) f, sum(c_float) over (partition by key order by value range between 1 preceding and current row) g, max(c_float) over (partition by key order by value range between 1 preceding and unbounded following) h, min(c_float) over (partition by key order by value range between 1 preceding and 1 following) i from cbo_t1) cbo_t1;
select *, rank() over(partition by key order by value) as rr from src1;
select *, rank() over(partition by key order by value) from src1;

