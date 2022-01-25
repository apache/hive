set hive.tez.union.flatten.subdirectories=true;

create external table union_src1 (value string) partitioned by (key int);
create external table union_src2 (value string) partitioned by (key int);
create external table union_target (value string) partitioned by (key int);

insert into table union_src1 partition (key = 1) values ("val1");
insert into table union_src1 partition (key = 2) values ("val2");

insert into table union_src2 partition (key = 1) values ("val3");
insert into table union_src2 partition (key = 3) values ("val4");

insert into table union_target
select value, key from union_src1
union all
select value, key from union_src2;

-- if there is any HIVE_UNION_SUBDIR, the output will have more rows
dfs -ls -R ${hiveconf:hive.metastore.warehouse.dir}/union_target;