--! qt:authorizer

dfs ${system:test.dfs.mkdir} hdfs:///tmp/ct_noperm_loc;
dfs -chmod 777 hdfs:///tmp;

set user.name=user1;

-- this will succeed because everybody can write to /tmp
create table foo0(id int) location 'hdfs:///tmp/ct_noperm_loc_foo0';

-- this will fail because user1 cannot write to /tmp/ct_noperm_loc
create table foo1(id int) location 'hdfs:///tmp/ct_noperm_loc/foo1';
