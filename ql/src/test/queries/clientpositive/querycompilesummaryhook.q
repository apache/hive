--! qt:replace:/( *)(\d+)ms/#Masked#/
set hive.query.lifetime.hooks=org.apache.hadoop.hive.ql.QueryCompilationSummaryHook;

create table tbl_n1 (n bigint, t string); 

explain
select n, t from tbl_n1 where n = 1;

