set hive.auto.convert.anti.join=false;
set hive.auto.convert.join.noconditionaltask.size=1145044992;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join=true;
set hive.auto.convert.sortmerge.join.to.mapjoin=true;
set hive.auto.convert.sortmerge.join=true;
set hive.cbo.enable=true;
set hive.convert.join.bucket.mapjoin.tez=true;
set hive.enforce.sortmergebucketmapjoin=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions.pernode=4000;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.parallel.thread.number=32;
set hive.exec.parallel=false;
set hive.exec.reducers.bytes.per.reducer=67108864;
set hive.exec.reducers.max=1009;
set hive.limit.optimize.enable=true;
set hive.limit.pushdown.memory.usage=0.04;
set hive.map.aggr.hash.force.flush.memory.threshold=0.9;
set hive.map.aggr.hash.min.reduction=0.99;
set hive.map.aggr.hash.percentmemory=0.5;
set hive.map.aggr=true;
set hive.mapjoin.bucket.cache.size=10000;
set hive.mapjoin.hybridgrace.hashtable=false;
set hive.mapjoin.optimized.hashtable=true;
set hive.mapred.reduce.tasks.speculative.execution=false;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=false;
set hive.merge.nway.joins=true;
set hive.merge.orcfile.stripe.level=true;
set hive.merge.rcfile.block.level=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.merge.tezfiles=false;
set hive.optimize.bucketmapjoin.sortedmerge=true;
set hive.optimize.bucketmapjoin=true;
set hive.optimize.constant.propagation=true;
set hive.optimize.cte.materialize.threshold=-1;
set hive.optimize.dynamic.partition.hashjoin=true;
set hive.optimize.index.filter=true;
set hive.optimize.metadataonly=true;
set hive.optimize.null.scan=true;
set hive.optimize.reducededuplication.min.reducer=4;
set hive.optimize.reducededuplication=true;
set hive.optimize.scan.probedecode=true;
set hive.optimize.shared.work.dppunion.merge.eventops=true;
set hive.optimize.shared.work.dppunion=true;
set hive.optimize.shared.work.extended=true;
set hive.optimize.shared.work.parallel.edge.support=true;
set hive.optimize.shared.work=true;
set hive.optimize.sort.dynamic.partition.threshold=0;
set hive.smbjoin.cache.rows=10000;
set hive.support.concurrency=true;
set hive.tez.bloom.filter.merge.threads=0;
set hive.tez.bucket.pruning=true;
set hive.tez.cartesian-product.enabled=false;
set hive.tez.dynamic.partition.pruning.max.data.size=104857600;
set hive.tez.dynamic.partition.pruning.max.event.size=1048576;
set hive.tez.dynamic.partition.pruning=true;
set hive.tez.input.generate.consistent.splits=true;
set hive.tez.smb.number.waves=0.5;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

set hive.tez.bigtable.minsize.semijoin.reduction=1;
set hive.tez.max.bloom.filter.entries=10000000000;
set hive.tez.dynamic.semijoin.reduction.threshold=0;

drop table if exists x1_store_sales;
drop table if exists x1_date_dim;

create table x1_store_sales (ss_sold_date_sk int, ss_item_sk int) stored as orc tblproperties('transactional'='true', 'transactional_properties'='default');
create table x1_date_dim (d_date_sk int, d_month_seq int, d_year int, d_moy int) stored as orc tblproperties('transactional'='true', 'transactional_properties'='default');

insert into x1_date_dim values (1,1,2000,1), (2,2,2001,2), (3,2,2001,3), (4,2,2001,4), (5,2,2001,5), (6,2,2001,6), (7,2,2001,7), (8,2,2001,8);
insert into x1_store_sales values (1,1),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10),(11,11);

alter table x1_store_sales update statistics set('numRows'='123456', 'rawDataSize'='1234567');
alter table x1_date_dim update statistics set('numRows'='28', 'rawDataSize'='81449');

explain
select ss_item_sk
from (
    select ss_item_sk, ss_sold_date_sk from x1_store_sales
    union all
    select ss_item_sk, ss_sold_date_sk from x1_store_sales
    union all
    select ss_item_sk, ss_sold_date_sk from x1_store_sales
  ) as tmp,
  x1_date_dim
where ss_sold_date_sk = d_date_sk and d_moy=1;

select ss_item_sk
from (
    select ss_item_sk, ss_sold_date_sk from x1_store_sales
    union all
    select ss_item_sk, ss_sold_date_sk from x1_store_sales
    union all
    select ss_item_sk, ss_sold_date_sk from x1_store_sales
  ) as tmp,
  x1_date_dim
where ss_sold_date_sk = d_date_sk and d_moy=1;


