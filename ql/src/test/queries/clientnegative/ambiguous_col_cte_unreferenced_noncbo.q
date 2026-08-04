set hive.cbo.enable=false;
with c1 as (select 'a' as c, 'b' as c, 'x' as d)
select d from c1;
