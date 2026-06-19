set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


create table acid_ivnp(ti tinyint,
                 si smallint,
                 i int,
                 bi bigint,
                 f float,
                 d double,
                 de decimal(5,2),
                 t timestamp,
                 dt date,
                 b boolean,
                 s string,
                 vc varchar(128),
                 ch char(12)) clustered by (i) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

insert into table acid_ivnp values 
    (1, 257, 65537, 4294967297, 3.14, 3.141592654, 109.23, '2014-08-25 17:21:30.0', '2014-08-25', true, 'mary had a little lamb', 'ring around the rosie', 'red'),
    (null, null, null, null, null, null, null, null, null, null, null, null, null),
    (3, 25, 6553, null, 0.14, 1923.141592654, 1.2301, '2014-08-24 17:21:30.0', '2014-08-26', false, 'its fleece was white as snow', 'a pocket full of posies', 'blue' );

select * from acid_ivnp order by ti;
