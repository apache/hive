-- where clause
select
  l_orderkey, l_shipdate, l_receiptdate
from lineitem
  where (cast(l_shipdate as date) - date '1992-01-01') < interval '365 0:0:0' day to second
order by l_orderkey;

select
  l_orderkey, l_shipdate, l_receiptdate
from lineitem
  where (cast(l_shipdate as date) + interval '1-0' year to month) <= date '1994-01-01'
order by l_orderkey;

select
  l_orderkey, l_shipdate, l_receiptdate
from lineitem
  where (cast(l_shipdate as date) + interval '1-0' year to month) <= date '1994-01-01'
    and (cast(l_receiptdate as date) - cast(l_shipdate as date)) < interval '10' day
order by l_orderkey;


-- joins
select
  a.l_orderkey, b.l_orderkey, a.interval1
from
  (
    select
      l_orderkey, l_shipdate, l_receiptdate, (cast(l_receiptdate as date) - cast(l_shipdate as date)) as interval1
    from lineitem
  ) a 
  join
  (
    select
      l_orderkey, l_shipdate, l_receiptdate, (cast(l_receiptdate as date) - date '1992-07-02') as interval2
    from lineitem
  ) b
  on a.interval1 = b.interval2 and a.l_orderkey = b.l_orderkey
order by a.l_orderkey;
