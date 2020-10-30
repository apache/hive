--! qt:dataset:impala_dataset

explain cbo physical select  a.ca_state state, count(*) cnt
 from impala_tpcds_customer_address a
     ,impala_tpcds_customer c
     ,impala_tpcds_store_sales s
     ,impala_tpcds_date_dim d
     ,impala_tpcds_item i
 where       a.ca_address_sk = c.c_current_addr_sk
      and c.c_customer_sk = s.ss_customer_sk
      and s.ss_sold_date_sk = d.d_date_sk
      and s.ss_item_sk = i.i_item_sk
      and d.d_month_seq = 
           (select distinct (d_month_seq)
            from impala_tpcds_date_dim
               where d_year = 2002
              and d_moy = 3 )
      and i.i_current_price > 1.2 * 
             (select avg(j.i_current_price) 
           from impala_tpcds_item j 
           where j.i_category = i.i_category)
 group by a.ca_state
 having count(*) >= 10
 order by cnt, a.ca_state 
 limit 100;

explain select  a.ca_state state, count(*) cnt
 from impala_tpcds_customer_address a
     ,impala_tpcds_customer c
     ,impala_tpcds_store_sales s
     ,impala_tpcds_date_dim d
     ,impala_tpcds_item i
 where       a.ca_address_sk = c.c_current_addr_sk
      and c.c_customer_sk = s.ss_customer_sk
      and s.ss_sold_date_sk = d.d_date_sk
      and s.ss_item_sk = i.i_item_sk
      and d.d_month_seq = 
           (select distinct (d_month_seq)
            from impala_tpcds_date_dim
               where d_year = 2002
              and d_moy = 3 )
      and i.i_current_price > 1.2 * 
             (select avg(j.i_current_price) 
           from impala_tpcds_item j 
           where j.i_category = i.i_category)
 group by a.ca_state
 having count(*) >= 10
 order by cnt, a.ca_state 
 limit 100;
