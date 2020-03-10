--! qt:authorizer
set hive.repl.rootdir=${system:test.tmp.dir}/hrepl;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/hrepl/sentinel;
dfs -rmr  ${system:test.tmp.dir}/hrepl;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/hrepl;

set user.name=hive_admin_user;
set role ADMIN;

drop database if exists test_replload_adminpriv_src cascade;
drop database if exists test_replload_adminpriv_tgt1 cascade;
drop database if exists test_replload_adminpriv_tgt2 cascade;

set user.name=ruser1;
show role grant user ruser1;

create database test_replload_adminpriv_src with DBPROPERTIES ('repl.source.for' = '1,2,3');
create table test_replload_adminpriv_src.dummy_tbl(a int) partitioned by (b string);
show tables test_replload_adminpriv_src;

set user.name=hive_admin_user;
set role ADMIN;
show role grant user ruser1;
show role grant user hive_admin_user;

-- repl dump
show tables test_replload_adminpriv_src;
repl dump test_replload_adminpriv_src;

-- repl load as admin should succeed
repl load test_replload_adminpriv_src into test_replload_adminpriv_tgt1;
show tables test_replload_adminpriv_tgt1;

set user.name=ruser1;

-- repl load as non-admin should fail
repl load test_replload_adminpriv_src into test_replload_adminpriv_tgt2;
