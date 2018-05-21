--! qt:dataset:src
set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS
create table jackson_sev_same_n0 as select * from src;
create table dim_pho_n0 as select * from src;
create table jackson_sev_add_n0 as select * from src;
explain select b.* from jackson_sev_same_n0 a join (select * from dim_pho_n0 union all select * from jackson_sev_add_n0)b on a.key=b.key and b.key=97;
select b.* from jackson_sev_same_n0 a join (select * from dim_pho_n0 union all select * from jackson_sev_add_n0)b on a.key=b.key and b.key=97;
