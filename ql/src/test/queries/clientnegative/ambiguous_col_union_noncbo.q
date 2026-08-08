set hive.cbo.enable=false;
select t.c from (select 'a' as c, 'b' as c union all select 'x', 'y') t;
