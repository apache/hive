set metastore.direct.product.name=fs;
set hive.stats.column.autogather=false;
with tempDataset as (
select 10 as account_id, cast(23.79 as double) interest_paid
union all select 10, 23.79
union all select 10, 23.79
union all select 11, 64.34
union all select 11, 64.34
union all select 11, 64.34
)
select account_id, STDDEV(interest_paid) as sdev, variance(interest_paid) as vari from tempDataset group by account_id;


create table cbo_test (key string, v1 double, v2 float, v3 decimal(30,2));
insert into cbo_test values ("001400000000000000000006375905", 10230.72, 10230.72, 10230.69), ("001400000000000000000006375905", 10230.72, 10230.72, 10230.69), ("001400000000000000000006375905", 10230.72, 10230.72, 10230.69), ("001400000000000000000006375905", 10230.72, 10230.72, 10230.69), ("001400000000000000000006375905", 10230.72, 10230.72, 10230.69), ("001400000000000000000006375905", 10230.72, 10230.72, 10230.69);

explain select stddev(v1), stddev(v2), stddev(v3) from cbo_test;
select stddev(v1), stddev(v2), stddev(v3) from cbo_test;

set hive.cbo.enable=false;
explain select stddev(v1), stddev(v2), stddev(v3) from cbo_test;
select stddev(v1), stddev(v2), stddev(v3) from cbo_test;
