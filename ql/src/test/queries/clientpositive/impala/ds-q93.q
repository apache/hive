--! qt:dataset:impala_dataset

explain cbo physical select  ss_customer_sk
            ,sum(act_sales) sumsales
      from (select ss_item_sk
                  ,ss_ticket_number
                  ,ss_customer_sk
                  ,case when sr_return_quantity is not null then (ss_quantity-sr_return_quantity)*ss_sales_price
                                                            else (ss_quantity*ss_sales_price) end act_sales
            from impala_tpcds_store_sales left outer join impala_tpcds_store_returns on (sr_item_sk = ss_item_sk
                                                               and sr_ticket_number = ss_ticket_number)
                ,impala_tpcds_reason
            where sr_reason_sk = r_reason_sk
              and r_reason_desc = 'reason 66') t
      group by ss_customer_sk
      order by sumsales, ss_customer_sk
limit 100;

explain select  ss_customer_sk
            ,sum(act_sales) sumsales
      from (select ss_item_sk
                  ,ss_ticket_number
                  ,ss_customer_sk
                  ,case when sr_return_quantity is not null then (ss_quantity-sr_return_quantity)*ss_sales_price
                                                            else (ss_quantity*ss_sales_price) end act_sales
            from impala_tpcds_store_sales left outer join impala_tpcds_store_returns on (sr_item_sk = ss_item_sk
                                                               and sr_ticket_number = ss_ticket_number)
                ,impala_tpcds_reason
            where sr_reason_sk = r_reason_sk
              and r_reason_desc = 'reason 66') t
      group by ss_customer_sk
      order by sumsales, ss_customer_sk
limit 100;
