--! qt:authorizer

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/authz_uri_load_data;
dfs -touchz ${system:test.tmp.dir}/authz_uri_load_data/1.txt;
dfs -chmod 555 ${system:test.tmp.dir}/authz_uri_load_data/1.txt;

create table t1(i int);
load data inpath 'pfile:${system:test.tmp.dir}/authz_uri_load_data/' overwrite into table t1;

