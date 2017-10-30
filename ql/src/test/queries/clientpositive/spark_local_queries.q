-- Some basic tests to test HoS works with spark.master = local

-- Test that a basic explain plan can be generated
explain select * from src order by key limit 10;

-- Test order by
select * from src order by key limit 10;

-- Test join
select * from src join src1 on src.key = src1.key order by src.key limit 10;

-- Test filer on partitioned table
select * from srcpart where ds = "2008-04-08" order by key limit 10;

-- Test group by
select key, count(*) from src group by key order by key limit 10;
