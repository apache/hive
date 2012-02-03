drop table ba_test;

-- this query tests all the udfs provided to work with binary works.

select length (cast(src.key as binary)) as len, concat(cast(src.key as binary), cast(src.value as binary)), substr(src.value, 5,1) as sub from src order by sub limit 10;

drop table ba_test;

