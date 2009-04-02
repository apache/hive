set hive.map.aggr.hash.percentmemory = 0.4;

select count(distinct subq.key) from
(FROM src MAP src.key USING 'python ../data/scripts/dumpdata_script.py' AS key WHERE src.key = 10) subq;
