set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table src_emptybucket_partitioned_1 (name string, age int, gpa decimal(3,2))
                               partitioned by(year int)
                               clustered by (age)
                               sorted by (age)
                               into 100 buckets
                               stored as orc tblproperties ("transactional"="true", "transactional_properties"="insert_only");

create table source_table(name string, age int, gpa decimal(3,2));
insert into source_table values("name", 56, 4);

explain insert into table src_emptybucket_partitioned_1 partition(year=2015) select * from source_table limit 0;
insert into table src_emptybucket_partitioned_1 partition(year=2015) select * from source_table limit 0;

insert into table src_emptybucket_partitioned_1 partition(year=2015) select * from source_table limit 1;
select * from src_emptybucket_partitioned_1;

drop table src_emptybucket_partitioned_1;
drop table source_table;
