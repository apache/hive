set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.vectorized.execution.enabled=true;
create external table test_decimal(rattag string, newclt_all decimal(15,2)) stored as orc;
insert into test_decimal values('a', '10.20');
explain vectorization detail select sum(case when rattag='a' then newclt_all*0.3 else newclt_all end) from test_decimal;
select sum(case when rattag='a' then newclt_all*0.3 else newclt_all end) from test_decimal;
explain vectorization detail select sum(case when rattag='Y' then newclt_all*0.3 else newclt_all end) from test_decimal;
select sum(case when rattag='Y' then newclt_all*0.3 else newclt_all end) from test_decimal;
