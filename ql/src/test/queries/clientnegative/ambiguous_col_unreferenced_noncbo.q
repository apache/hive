set hive.cbo.enable=false;
select t.d from (select 'a' as c, 'b' as c, 'x' as d) t;
