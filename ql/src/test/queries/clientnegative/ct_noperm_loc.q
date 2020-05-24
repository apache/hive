--! qt:authorizer

dfs ${system:test.dfs.mkdir} hdfs:///tmp/ct_noperm_loc;

set user.name=user1;

create table foo0(id int) location 'hdfs:///tmp/ct_noperm_loc_foo0';
create table foo1(id int) location 'hdfs:///tmp/ct_noperm_loc/foo1';
