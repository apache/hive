--! qt:dataset:src
--! qt:dataset:cbo_t3
set hive.mapred.mode=nonstrict;
set hive.cbo.enable=true;
set hive.exec.check.crossproducts=false;

set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join=false;

-- 21. Test groupby is empty and there is no other cols in aggr
select unionsrc.key FROM (select 'tst1' as key, count(1) as value from src) unionsrc;

select unionsrc.key, unionsrc.value FROM (select 'tst1' as key, count(1) as value from src) unionsrc;

select unionsrc.key FROM (select 'max' as key, max(c_int) as value from cbo_t3 s1
	UNION  ALL
    	select 'min' as key,  min(c_int) as value from cbo_t3 s2
    UNION ALL
        select 'avg' as key,  avg(c_int) as value from cbo_t3 s3) unionsrc order by unionsrc.key;
        
select unionsrc.key, unionsrc.value FROM (select 'max' as key, max(c_int) as value from cbo_t3 s1
	UNION  ALL
    	select 'min' as key,  min(c_int) as value from cbo_t3 s2
    UNION ALL
        select 'avg' as key,  avg(c_int) as value from cbo_t3 s3) unionsrc order by unionsrc.key;

select unionsrc.key, count(1) FROM (select 'max' as key, max(c_int) as value from cbo_t3 s1
    UNION  ALL
        select 'min' as key,  min(c_int) as value from cbo_t3 s2
    UNION ALL
        select 'avg' as key,  avg(c_int) as value from cbo_t3 s3) unionsrc group by unionsrc.key order by unionsrc.key;

