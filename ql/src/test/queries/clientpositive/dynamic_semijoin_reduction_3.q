set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.explain.user=false;
set hive.merge.cardinality.check=true;

set hive.compute.query.using.stats=false;
set hive.mapred.mode=nonstrict;
set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;
set hive.tez.dynamic.partition.pruning=true;
set hive.tez.dynamic.semijoin.reduction=true;
set hive.optimize.metadataonly=false;
set hive.optimize.index.filter=true;
set hive.tez.bigtable.minsize.semijoin.reduction=1;
set hive.tez.min.bloom.filter.entries=1;
set hive.tez.dynamic.semijoin.reduction.threshold=-999999999999;


-- Try with merge statements
create table acidTbl(a int, b int) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
create table nonAcidOrcTbl(a int, b int) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='false');

--expect a cardinality check because there is update and hive.merge.cardinality.check=true by default
explain merge into acidTbl as t using nonAcidOrcTbl s ON t.a = s.a 
WHEN MATCHED AND s.a > 8 THEN DELETE
WHEN MATCHED THEN UPDATE SET b = 7
WHEN NOT MATCHED THEN INSERT VALUES(s.a, s.b);

--now we expect no cardinality check since only have insert clause
explain merge into acidTbl as t using nonAcidOrcTbl s ON t.a = s.a
WHEN NOT MATCHED THEN INSERT VALUES(s.a, s.b);

explain merge into acidTbl as t using (
  select * from nonAcidOrcTbl where a > 0
  union all
  select * from nonAcidOrcTbl where b > 0
) s ON t.a = s.a
WHEN MATCHED AND s.a > 8 THEN DELETE
WHEN MATCHED THEN UPDATE SET b = 7
WHEN NOT MATCHED THEN INSERT VALUES(s.a, s.b);

--HIVE-16211
drop database if exists type2_scd_helper cascade;
create database type2_scd_helper;
use type2_scd_helper;

drop table if exists customer;
drop table if exists customer_updates;
drop table if exists new_customer_stage;

create table customer (
source_pk int,
sk string,
name string,
state string,
is_current boolean,
end_date date
)
CLUSTERED BY (sk) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES ("transactional"="true");

insert into customer values
                     ( 1, "ABC", "Abc Co.", "OH", true, null ),
                     ( 2, "DEF", "Def Co.", "PA", true, null ),
                     ( 3, "XYZ", "Xyz Co.", "CA", true, null );
select * from customer order by source_pk;

create table new_customer_stage (
source_pk int,
name string,
state string
);
insert into new_customer_stage values
                               ( 1, "Abc Co.", "OH" ),
                               ( 2, "Def Co.", "PA" ),
                               ( 3, "Xyz Co.", "TX" ),
                               ( 4, "Pdq Co.", "WI" );

drop table if exists scd_types;
create table scd_types (
type int,
invalid_key int
);
insert into scd_types values (1, null), (2, -1), (2, null);

merge into customer
using (
  select
    *,
    coalesce(invalid_key, source_pk) as join_key
  from (
    select
      stage.source_pk, stage.name, stage.state,
      case when customer.source_pk is null then 1
      when stage.name <> customer.name or stage.state <> customer.state then 2
      else 0 end as scd_row_type
    from
      new_customer_stage stage
    left join
      customer
    on (stage.source_pk = customer.source_pk and customer.is_current = true)
  ) updates
  join scd_types on scd_types.type = scd_row_type
) sub
on sub.join_key = customer.source_pk
when matched then update set
  is_current = false,
  end_date = date '2017-03-15' 
when not matched then insert values
  (sub.source_pk, upper(substr(sub.name, 0, 3)), sub.name, sub.state, true, null);
select * from customer order by source_pk;

drop table customer;
drop table customer_updates;
drop table new_customer_stage;
drop table scd_types;
