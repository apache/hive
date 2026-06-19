--! qt:authorizer

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/authz_drop_part_1;

-- check drop partition without delete privilege
create table tpart(i int, j int) partitioned by (k string);
alter table tpart add partition (k = 'abc') location 'file:${system:test.tmp.dir}/authz_drop_part_1' ;
set user.name=user1;
alter table tpart drop partition (k = 'abc');
