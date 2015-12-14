set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


create temporary table acid_ivtt(i int, de decimal(5,2), vc varchar(128)) clustered by (vc) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

insert into table acid_ivtt values 
    (1, 109.23, 'mary had a little lamb'),
    (429496729, 0.14, 'its fleece was white as snow'),
    (-29496729, -0.14, 'negative values test');

select i, de, vc from acid_ivtt order by i;
