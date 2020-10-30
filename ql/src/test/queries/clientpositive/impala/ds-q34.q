--! qt:dataset:impala_dataset

explain cbo physical select c_last_name
       ,c_first_name
       ,c_salutation
       ,c_preferred_cust_flag
       ,ss_ticket_number
       ,cnt from
   (select ss_ticket_number
          ,ss_customer_sk
          ,count(*) cnt
    from impala_tpcds_store_sales,impala_tpcds_date_dim,impala_tpcds_store,impala_tpcds_household_demographics
    where impala_tpcds_store_sales.ss_sold_date_sk = impala_tpcds_date_dim.d_date_sk
    and impala_tpcds_store_sales.ss_store_sk = impala_tpcds_store.s_store_sk  
    and impala_tpcds_store_sales.ss_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk
    and (impala_tpcds_date_dim.d_dom between 1 and 3 or impala_tpcds_date_dim.d_dom between 25 and 28)
    and (impala_tpcds_household_demographics.hd_buy_potential = '>10000' or
         impala_tpcds_household_demographics.hd_buy_potential = 'Unknown')
    and impala_tpcds_household_demographics.hd_vehicle_count > 0
    and (case when impala_tpcds_household_demographics.hd_vehicle_count > 0 
     then impala_tpcds_household_demographics.hd_dep_count/ impala_tpcds_household_demographics.hd_vehicle_count 
     else null 
     end)  > 1.2
    and impala_tpcds_date_dim.d_year in (2000,2000+1,2000+2)
    and impala_tpcds_store.s_county in ('Salem County','Terrell County','Arthur County','Oglethorpe County',
                           'Lunenburg County','Perry County','Halifax County','Sumner County')
    group by ss_ticket_number,ss_customer_sk) dn,impala_tpcds_customer
    where ss_customer_sk = c_customer_sk
      and cnt between 15 and 20
    order by c_last_name,c_first_name,c_salutation,c_preferred_cust_flag desc, ss_ticket_number;

explain select c_last_name
       ,c_first_name
       ,c_salutation
       ,c_preferred_cust_flag
       ,ss_ticket_number
       ,cnt from
   (select ss_ticket_number
          ,ss_customer_sk
          ,count(*) cnt
    from impala_tpcds_store_sales,impala_tpcds_date_dim,impala_tpcds_store,impala_tpcds_household_demographics
    where impala_tpcds_store_sales.ss_sold_date_sk = impala_tpcds_date_dim.d_date_sk
    and impala_tpcds_store_sales.ss_store_sk = impala_tpcds_store.s_store_sk  
    and impala_tpcds_store_sales.ss_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk
    and (impala_tpcds_date_dim.d_dom between 1 and 3 or impala_tpcds_date_dim.d_dom between 25 and 28)
    and (impala_tpcds_household_demographics.hd_buy_potential = '>10000' or
         impala_tpcds_household_demographics.hd_buy_potential = 'Unknown')
    and impala_tpcds_household_demographics.hd_vehicle_count > 0
    and (case when impala_tpcds_household_demographics.hd_vehicle_count > 0 
     then impala_tpcds_household_demographics.hd_dep_count/ impala_tpcds_household_demographics.hd_vehicle_count 
     else null 
     end)  > 1.2
    and impala_tpcds_date_dim.d_year in (2000,2000+1,2000+2)
    and impala_tpcds_store.s_county in ('Salem County','Terrell County','Arthur County','Oglethorpe County',
                           'Lunenburg County','Perry County','Halifax County','Sumner County')
    group by ss_ticket_number,ss_customer_sk) dn,impala_tpcds_customer
    where ss_customer_sk = c_customer_sk
      and cnt between 15 and 20
    order by c_last_name,c_first_name,c_salutation,c_preferred_cust_flag desc, ss_ticket_number;
