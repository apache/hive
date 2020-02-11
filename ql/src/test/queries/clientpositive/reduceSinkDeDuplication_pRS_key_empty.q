--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.cbo.enable=false;

set hive.map.aggr=false;

set hive.groupby.skewindata=false;
set mapred.reduce.tasks=31;


select compute_stats(a,'fm',16),compute_stats(b,'fm',16),compute_stats(c,'fm',16),compute_stats(d,'fm',16)
from
(
select
  avg(substr(src.value,5)) as a,
  max(substr(src.value,5)) as b,
  variance(substr(src.value,5)) as c,
  var_samp(substr(src.value,5)) as d
 from src)subq;

explain select compute_stats(a,'fm',16),compute_stats(b,'fm',16),compute_stats(c,'fm',16),compute_stats(d,'fm',16)
from
(
select
  avg(DISTINCT substr(src.value,5)) as a,
  max(substr(src.value,5)) as b,
  variance(substr(src.value,5)) as c,
  var_samp(substr(src.value,5)) as d
 from src)subq;

select compute_stats(a,'fm',16),compute_stats(b,'fm',16),compute_stats(c,'fm',16),compute_stats(d,'fm',16)
from
(
select
  avg(DISTINCT substr(src.value,5)) as a,
  max(substr(src.value,5)) as b,
  variance(substr(src.value,5)) as c,
  var_samp(substr(src.value,5)) as d
 from src)subq;
 
set hive.optimize.reducededuplication=false;

explain select compute_stats(a,'fm',16),compute_stats(b,'fm',16),compute_stats(c,'fm',16),compute_stats(d,'fm',16)
from
(
select
  avg(DISTINCT substr(src.value,5)) as a,
  max(substr(src.value,5)) as b,
  variance(substr(src.value,5)) as c,
  var_samp(substr(src.value,5)) as d
 from src)subq;

select compute_stats(a,'fm',16),compute_stats(b,'fm',16),compute_stats(c,'fm',16),compute_stats(d,'fm',16)
from
(
select
  avg(DISTINCT substr(src.value,5)) as a,
  max(substr(src.value,5)) as b,
  variance(substr(src.value,5)) as c,
  var_samp(substr(src.value,5)) as d
 from src)subq;
