set hive.cbo.fallback.strategy=CONSERVATIVE;
select cast(0 as bigint) = '1' except select cast(1 as bigint) = '1';
