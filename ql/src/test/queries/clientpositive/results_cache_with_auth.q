
-- Setup results cache
set hive.compute.query.using.stats=false;
set hive.query.results.cache.enabled=true;
set hive.query.results.cache.nontransactional.tables.enabled=true;

-- Setup auth
set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

create table results_cache_with_auth_t1 (c1 string);
insert into results_cache_with_auth_t1 values ('abc');

explain
select count(*) from results_cache_with_auth_t1;

select count(*) from results_cache_with_auth_t1;

set test.comment="Cache should be used for this query";
set test.comment;
explain
select count(*) from results_cache_with_auth_t1;

select count(*) from results_cache_with_auth_t1;

set hive.security.authorization.enabled=false;
drop table results_cache_with_auth_t1;
