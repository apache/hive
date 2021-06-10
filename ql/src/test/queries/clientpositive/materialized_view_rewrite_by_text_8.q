set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.materializedview.rewriting=false;

create table t1(col0 int) STORED AS ORC
                          TBLPROPERTIES ('transactional'='true');

create materialized view mat1 as
select col0 from t1 where col0 > 1;

create materialized view mat2 as
select col0 from
  (select col0 from t1 where col0 > 1) sub
where col0 < 100;

explain cbo
select col0 from
  (select col0 from t1 where col0 > 1) sub
where col0 < 100;

explain cbo
select col0 from (
select col0 from
  (select col0 from t1 where col0 > 1) sub
where col0 < 100
) sub2
where col0 = 10;

explain cbo
select col0 from (
    select col0 from
      (select col0 from t1 where col0 > 1) sub
    where col0 < 100
) sub2
where col0 = 10;

explain cbo
select col0 from t1 where col0 in (
select col0 from
  (select col0 from t1 where col0 > 1) sub
where col0 < 100
);

explain cbo
select col0 from t1 where col0 in (
  select col0 from
    (select col0 from t1 where col0 > 1) sub
where col0 < 100
);

drop materialized view mat2;
drop materialized view mat1;
