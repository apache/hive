--! qt:dataset:impala_dataset

explain cbo physical select  
  cd_gender,
  cd_marital_status,
  cd_education_status,
  count(*) cnt1,
  cd_purchase_estimate,
  count(*) cnt2,
  cd_credit_rating,
  count(*) cnt3
 from
  impala_tpcds_customer c,impala_tpcds_customer_address ca,impala_tpcds_customer_demographics
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  ca_state in ('MO','MN','AZ') and
  cd_demo_sk = c.c_current_cdemo_sk and 
  exists (select *
          from impala_tpcds_store_sales,impala_tpcds_date_dim
          where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                d_year = 2003 and
                d_moy between 2 and 2+2) and
   (not exists (select *
            from impala_tpcds_web_sales,impala_tpcds_date_dim
            where c.c_customer_sk = ws_bill_customer_sk and
                  ws_sold_date_sk = d_date_sk and
                  d_year = 2003 and
                  d_moy between 2 and 2+2) and
    not exists (select * 
            from impala_tpcds_catalog_sales,impala_tpcds_date_dim
            where c.c_customer_sk = cs_ship_customer_sk and
                  cs_sold_date_sk = d_date_sk and
                  d_year = 2003 and
                  d_moy between 2 and 2+2))
 group by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
 order by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
 limit 100;

explain select  
  cd_gender,
  cd_marital_status,
  cd_education_status,
  count(*) cnt1,
  cd_purchase_estimate,
  count(*) cnt2,
  cd_credit_rating,
  count(*) cnt3
 from
  impala_tpcds_customer c,impala_tpcds_customer_address ca,impala_tpcds_customer_demographics
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  ca_state in ('MO','MN','AZ') and
  cd_demo_sk = c.c_current_cdemo_sk and 
  exists (select *
          from impala_tpcds_store_sales,impala_tpcds_date_dim
          where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                d_year = 2003 and
                d_moy between 2 and 2+2) and
   (not exists (select *
            from impala_tpcds_web_sales,impala_tpcds_date_dim
            where c.c_customer_sk = ws_bill_customer_sk and
                  ws_sold_date_sk = d_date_sk and
                  d_year = 2003 and
                  d_moy between 2 and 2+2) and
    not exists (select * 
            from impala_tpcds_catalog_sales,impala_tpcds_date_dim
            where c.c_customer_sk = cs_ship_customer_sk and
                  cs_sold_date_sk = d_date_sk and
                  d_year = 2003 and
                  d_moy between 2 and 2+2))
 group by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
 order by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
 limit 100;
