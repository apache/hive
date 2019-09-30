set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


-- SORT_QUERY_RESULTS

create table acid_uami(i int,
                 de decimal(5,2),
                 vc varchar(128)) clustered by (i) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

insert into table acid_uami values 
    (1, 109.23, 'mary had a little lamb'),
    (6553, 923.19, 'its fleece was white as snow');

insert into table acid_uami values 
    (10, 119.23, 'and everywhere that mary went'),
    (65530, 823.19, 'the lamb was sure to go');

select * from acid_uami order by de;

update acid_uami set de = 3.14 where de = 109.23 or de = 119.23;

select * from acid_uami order by de;
