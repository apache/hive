select x.c from (select distinct *, rank() over (order by d) r from (select 'a' as c, 'b' as c, 'x' as d) t) x;
