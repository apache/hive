--! qt:authorizer

dfs ${system:test.dfs.mkdir} hdfs:///tmp/ctas_noperm_loc;

set user.name=user1;

create table foo0 location 'hdfs:///tmp/ctas_noperm_loc_foo0' as select 1 as c1;
create table foo1 location 'hdfs:///tmp/ctas_noperm_loc/foo1' as select 1 as c1;
