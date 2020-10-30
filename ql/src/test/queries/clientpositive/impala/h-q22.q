--! qt:dataset:impala_dataset

explain cbo physical select
  cntrycode,
  count(*) as numcust,
  sum(c_acctbal) as totacctbal
from (
  select
    substr(c_phone, 1, 2) as cntrycode,
    c_acctbal
  from
    impala_tpch_customer
  where
    substr(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')
    and c_acctbal > (
      select 
        avg(c_acctbal)
      from
        impala_tpch_customer
      where
        c_acctbal > 0.00
        and substr(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')
      )
    and not exists (
      select
        *
      from
        impala_tpch_orders
      where
        o_custkey = c_custkey
    ) 
  ) as custsale
group by
  cntrycode
order by
  cntrycode;

explain select
  cntrycode,
  count(*) as numcust,
  sum(c_acctbal) as totacctbal
from (
  select
    substr(c_phone, 1, 2) as cntrycode,
    c_acctbal
  from
    impala_tpch_customer
  where
    substr(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')
    and c_acctbal > (
      select 
        avg(c_acctbal)
      from
        impala_tpch_customer
      where
        c_acctbal > 0.00
        and substr(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')
      )
    and not exists (
      select
        *
      from
        impala_tpch_orders
      where
        o_custkey = c_custkey
    ) 
  ) as custsale
group by
  cntrycode
order by
  cntrycode;
