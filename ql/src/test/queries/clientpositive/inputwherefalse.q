From (select * from src) a
insert overwrite directory 'target/warehouse/destemp.out/dir1/'
select key
where key=200 limit 1
insert overwrite directory 'target/warehouse/destemp.out/dir2/'
select 'header'
where 1=2
insert overwrite directory 'target/warehouse/destemp.out/dir3/'
select key
where key = 100 limit 1;

dfs -cat ${system:test.warehouse.dir}/destemp.out/dir1/000000_0;
dfs -cat ${system:test.warehouse.dir}/destemp.out/dir2/000000_0;
dfs -cat ${system:test.warehouse.dir}/destemp.out/dir3/000000_0;
dfs -rmr ${system:test.warehouse.dir}/destemp.out;
