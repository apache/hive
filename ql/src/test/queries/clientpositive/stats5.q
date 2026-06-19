--! qt:dataset:src
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
set datanucleus.cache.collections=false;

create table analyze_src as select * from src;

explain analyze table analyze_src compute statistics;

analyze table analyze_src compute statistics;

describe formatted analyze_src;
