--! qt:authorizer

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/a_uri_crtab1;
dfs -touchz ${system:test.tmp.dir}/a_uri_crtab1/1.txt;
dfs -chmod 555 ${system:test.tmp.dir}/a_uri_crtab1/1.txt;

create table t1(i int) location '${system:test.tmp.dir}/a_uri_crtab1';

-- Attempt to create table with dir that does not have write permission should fail
