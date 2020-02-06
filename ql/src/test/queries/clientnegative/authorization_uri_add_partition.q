--! qt:authorizer

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/a_uri_add_part;
dfs -touchz ${system:test.tmp.dir}/a_uri_add_part/1.txt;
dfs -chmod 555 ${system:test.tmp.dir}/a_uri_add_part/1.txt;

create table tpart(i int, j int) partitioned by (k string);
alter table tpart add partition (k = 'abc') location '${system:test.tmp.dir}/a_uri_add_part/';
