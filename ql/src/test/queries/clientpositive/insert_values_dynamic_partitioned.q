SET hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


create table ivdp(i int,
                 de decimal(5,2),
                 vc varchar(128)) partitioned by (ds string) clustered by (i) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

insert into table ivdp partition (ds) values 
    (1, 109.23, 'and everywhere that mary went', 'today'),
    (6553, 923.19, 'the lamb was sure to go', 'tomorrow');

select * from ivdp order by ds;
