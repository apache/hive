set hive.cbo.enable=false;
select * from (select * from (select 'a' as c, 'b' as c) a) b;
