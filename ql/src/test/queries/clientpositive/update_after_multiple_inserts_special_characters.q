set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


-- SORT_QUERY_RESULTS

create table `aci/d_u/ami`(i int,
                 `d?*de e` decimal(5,2),
                 vc varchar(128)) clustered by (i) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

insert into table `aci/d_u/ami` values 
    (1, 109.23, 'mary had a little lamb'),
    (6553, 923.19, 'its fleece was white as snow');

insert into table `aci/d_u/ami` values 
    (10, 119.23, 'and everywhere that mary went'),
    (65530, 823.19, 'the lamb was sure to go');

select * from `aci/d_u/ami` order by `d?*de e`;

update `aci/d_u/ami` set `d?*de e` = 3.14 where `d?*de e` = 109.23 or `d?*de e` = 119.23;

select * from `aci/d_u/ami` order by `d?*de e`;
