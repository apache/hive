set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.materializedview.rewriting=false;

create table t1(col0 int) STORED AS ORC
                          TBLPROPERTIES ('transactional'='true');

create materialized view mat1 as
select l.col0 from t1 l left outer join t1 r on (l.col0 = r.col0) where l.col0 = 20;

-- create MV mat2 contains mat1 definiton as subquery, no rewrite
create materialized view mat2 as
select col0 from
  (select l.col0 from t1 l left outer join t1 r on (l.col0 = r.col0) where l.col0 = 20) sub
where col0 < 100;

-- rewrite to scan mat2
explain cbo
select col0 from
  (select l.col0 from t1 l left outer join t1 r on (l.col0 = r.col0) where l.col0 = 20) sub
where col0 < 100;

-- rewrite subquery sub2 to scan mat2
explain cbo
select col0 from (
select col0 from
  (select l.col0 from t1 l left outer join t1 r on (l.col0 = r.col0) where l.col0 = 20) sub
where col0 < 100
) sub2
where col0 = 10;

-- rewrite subquery sub to scan mat1, can not rewrite to scan mat2 because indentation does not match
explain cbo
select col0 from (
    select col0 from
      (select l.col0 from t1 l left outer join t1 r on (l.col0 = r.col0) where l.col0 = 20) sub
    where col0 < 100
) sub2
where col0 = 10;

-- rewrite subquery sub2 to scan mat2
explain cbo
select col0 from t1 where col0 in (
  select col0 from
    (select l.col0 from t1 l left outer join t1 r on (l.col0 = r.col0) where l.col0 = 20) sub
where col0 < 100
);

drop materialized view mat2;
drop materialized view mat1;
