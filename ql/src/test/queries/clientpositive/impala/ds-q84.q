--! qt:dataset:impala_dataset

explain cbo select  c_customer_id as customer_id
       , coalesce(c_last_name,'') || ', ' || coalesce(c_first_name,'') as customername
 from impala_tpcds_customer
     ,impala_tpcds_customer_address
     ,impala_tpcds_customer_demographics
     ,impala_tpcds_household_demographics
     ,impala_tpcds_income_band
     ,impala_tpcds_store_returns
 where ca_city             =  'Hopewell'
   and c_current_addr_sk = ca_address_sk
   and ib_lower_bound   >=  37855
   and ib_upper_bound   <=  37855 + 50000
   and ib_income_band_sk = hd_income_band_sk
   and cd_demo_sk = c_current_cdemo_sk
   and hd_demo_sk = c_current_hdemo_sk
   and sr_cdemo_sk = cd_demo_sk
 order by c_customer_id
 limit 100;

explain select  c_customer_id as customer_id
       , coalesce(c_last_name,'') || ', ' || coalesce(c_first_name,'') as customername
 from impala_tpcds_customer
     ,impala_tpcds_customer_address
     ,impala_tpcds_customer_demographics
     ,impala_tpcds_household_demographics
     ,impala_tpcds_income_band
     ,impala_tpcds_store_returns
 where ca_city             =  'Hopewell'
   and c_current_addr_sk = ca_address_sk
   and ib_lower_bound   >=  37855
   and ib_upper_bound   <=  37855 + 50000
   and ib_income_band_sk = hd_income_band_sk
   and cd_demo_sk = c_current_cdemo_sk
   and hd_demo_sk = c_current_hdemo_sk
   and sr_cdemo_sk = cd_demo_sk
 order by c_customer_id
 limit 100;
