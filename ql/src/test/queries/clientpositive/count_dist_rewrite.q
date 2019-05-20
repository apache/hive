--! qt:dataset:src
set hive.optimize.metadataonly=true;

explain select count(distinct key) from src; 

select count(distinct key) from src; 

explain select max(key), count(distinct key) B1_CNTD from src; 

select max(key), count(distinct key) B1_CNTD from src; 

explain select max(key), count(distinct key), min(key) from src; 

select max(key), count(distinct key), min(key) from src; 

explain select max(key), count(distinct key), min(key), avg(key) from src;

select max(key), count(distinct key), min(key), avg(key) from src;

explain select count(1), count(distinct key) from src;

select count(1), count(distinct key) from src;

explain select 
  count(*) as total,
  count(key) as not_null_total,
  count(distinct key) as unique_days,
  max(value) as max_ss_store_sk,
  max(key) as max_ss_promo_sk
from src;

select
  count(*) as total,
  count(key) as not_null_total,
  count(distinct key) as unique_days,
  max(value) as max_ss_store_sk,
  max(key) as max_ss_promo_sk
from src;

explain select count(1), count(distinct key), cast(STDDEV(key) as int) from src;
select count(1), count(distinct key), cast(STDDEV(key) as int) from src;
select count(distinct key), count(1), cast(STDDEV(key) as int) from src;

explain SELECT
  sum(substr(src.value,5)),
  avg(substr(src.value,5)),
  count(DISTINCT substr(src.value,5)),
  max(substr(src.value,5)),
  min(substr(src.value,5)),
  cast(std(substr(src.value,5)) as int),
  cast(stddev_samp(substr(src.value,5)) as int),
  cast(variance(substr(src.value,5)) as int),
  cast(var_samp(substr(src.value,5)) as int)  from src;

SELECT
  sum(substr(src.value,5)),
  avg(substr(src.value,5)),
  count(DISTINCT substr(src.value,5)),
  max(substr(src.value,5)),
  min(substr(src.value,5)),
  cast(std(substr(src.value,5)) as int),
  cast(stddev_samp(substr(src.value,5)) as int),
  cast(variance(substr(src.value,5)) as int),
  cast(var_samp(substr(src.value,5)) as int)  from src;

explain select max(key), count(distinct key), min(key), avg(key) from src group by value;

select max(key), count(distinct key), min(key), avg(key) from src group by value;
