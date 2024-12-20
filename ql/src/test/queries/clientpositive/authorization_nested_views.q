--! qt:dataset:src

set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;

-- create a table
create table src_autho_test as select * from src;

-- create a view
create view v1 as select * from src_autho_test;

-- create a second view by simple select query
create view v2 as select * from v1;

-- create a third view by with clause
create view v3 as with t as (select * from v1) select * from t;

set hive.security.authorization.enabled=true;

-- grant access to the views barring the source view and table.

grant select on table v2 to user hive_test_user;
grant select on table v3 to user hive_test_user;

explain authorization select * from v2;
explain authorization select * from v3;

-- try reading from the views
select * from v2 order by key LIMIT 10;

select * from v3 order by key LIMIT 10;


