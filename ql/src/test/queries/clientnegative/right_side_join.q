--! qt:dataset:alltypesorc
set hive.cbo.enable=false;

explain  
select *
 from alltypesorc,
(
 (select csmallint from alltypesorc) a
 left join
 (select csmallint from alltypesorc) b
 on a.csmallint = b.csmallint
);

