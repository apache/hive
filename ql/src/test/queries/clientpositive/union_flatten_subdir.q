set hive.tez.union.flatten.subdirectories=true;

create external table src1 (value string) partitioned by (key int);
create external table src2 (value string) partitioned by (key int);
create external table target (value string) partitioned by (key int);

insert into table src1 partition (key = 1) values ("val1");
insert into table src1 partition (key = 2) values ("val2");

insert into table src2 partition (key = 1) values ("val3");
insert into table src2 partition (key = 3) values ("val4");

insert into table target
select value, key from src1
union all
select value, key from src2;

-- if there is any HIVE_UNION_SUBDIR, the output will have more rows
dfs -ls -R ${hiveconf:hive.metastore.warehouse.dir}/target;