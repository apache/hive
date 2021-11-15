--! qt:authorizer

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/a_uri_crtab_ext;
dfs -touchz ${system:test.tmp.dir}/a_uri_crtab_ext/1.txt;
dfs -chmod 555 ${system:test.tmp.dir}/a_uri_crtab_ext/1.txt;

create external table t1(i int) location '${system:test.tmp.dir}/a_uri_crtab_ext';

-- Attempt to create table with dir that does not have write permission should fail
