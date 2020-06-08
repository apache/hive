--! qt:dataset:impala_dataset

explain cbo select  *
from
 (select count(*) h8_30_to_9
 from impala_tpcds_store_sales, impala_tpcds_household_demographics , impala_tpcds_time_dim, impala_tpcds_store
 where ss_sold_time_sk = impala_tpcds_time_dim.t_time_sk   
     and ss_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk 
     and ss_store_sk = s_store_sk
     and impala_tpcds_time_dim.t_hour = 8
     and impala_tpcds_time_dim.t_minute >= 30
     and ((impala_tpcds_household_demographics.hd_dep_count = 2 and impala_tpcds_household_demographics.hd_vehicle_count<=2+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 4 and impala_tpcds_household_demographics.hd_vehicle_count<=4+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 3 and impala_tpcds_household_demographics.hd_vehicle_count<=3+2)) 
     and impala_tpcds_store.s_store_name = 'ese') s1,
 (select count(*) h9_to_9_30 
 from impala_tpcds_store_sales, impala_tpcds_household_demographics , impala_tpcds_time_dim, impala_tpcds_store
 where ss_sold_time_sk = impala_tpcds_time_dim.t_time_sk
     and ss_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk 
     and impala_tpcds_time_dim.t_hour = 9 
     and impala_tpcds_time_dim.t_minute < 30
     and ((impala_tpcds_household_demographics.hd_dep_count = 2 and impala_tpcds_household_demographics.hd_vehicle_count<=2+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 4 and impala_tpcds_household_demographics.hd_vehicle_count<=4+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 3 and impala_tpcds_household_demographics.hd_vehicle_count<=3+2))
     and impala_tpcds_store.s_store_name = 'ese') s2,
 (select count(*) h9_30_to_10 
 from impala_tpcds_store_sales, impala_tpcds_household_demographics , impala_tpcds_time_dim, impala_tpcds_store
 where ss_sold_time_sk = impala_tpcds_time_dim.t_time_sk
     and ss_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and impala_tpcds_time_dim.t_hour = 9
     and impala_tpcds_time_dim.t_minute >= 30
     and ((impala_tpcds_household_demographics.hd_dep_count = 2 and impala_tpcds_household_demographics.hd_vehicle_count<=2+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 4 and impala_tpcds_household_demographics.hd_vehicle_count<=4+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 3 and impala_tpcds_household_demographics.hd_vehicle_count<=3+2))
     and impala_tpcds_store.s_store_name = 'ese') s3,
 (select count(*) h10_to_10_30
 from impala_tpcds_store_sales, impala_tpcds_household_demographics , impala_tpcds_time_dim, impala_tpcds_store
 where ss_sold_time_sk = impala_tpcds_time_dim.t_time_sk
     and ss_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and impala_tpcds_time_dim.t_hour = 10 
     and impala_tpcds_time_dim.t_minute < 30
     and ((impala_tpcds_household_demographics.hd_dep_count = 2 and impala_tpcds_household_demographics.hd_vehicle_count<=2+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 4 and impala_tpcds_household_demographics.hd_vehicle_count<=4+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 3 and impala_tpcds_household_demographics.hd_vehicle_count<=3+2))
     and impala_tpcds_store.s_store_name = 'ese') s4,
 (select count(*) h10_30_to_11
 from impala_tpcds_store_sales, impala_tpcds_household_demographics , impala_tpcds_time_dim, impala_tpcds_store
 where ss_sold_time_sk = impala_tpcds_time_dim.t_time_sk
     and ss_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and impala_tpcds_time_dim.t_hour = 10 
     and impala_tpcds_time_dim.t_minute >= 30
     and ((impala_tpcds_household_demographics.hd_dep_count = 2 and impala_tpcds_household_demographics.hd_vehicle_count<=2+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 4 and impala_tpcds_household_demographics.hd_vehicle_count<=4+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 3 and impala_tpcds_household_demographics.hd_vehicle_count<=3+2))
     and impala_tpcds_store.s_store_name = 'ese') s5,
 (select count(*) h11_to_11_30
 from impala_tpcds_store_sales, impala_tpcds_household_demographics , impala_tpcds_time_dim, impala_tpcds_store
 where ss_sold_time_sk = impala_tpcds_time_dim.t_time_sk
     and ss_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk 
     and impala_tpcds_time_dim.t_hour = 11
     and impala_tpcds_time_dim.t_minute < 30
     and ((impala_tpcds_household_demographics.hd_dep_count = 2 and impala_tpcds_household_demographics.hd_vehicle_count<=2+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 4 and impala_tpcds_household_demographics.hd_vehicle_count<=4+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 3 and impala_tpcds_household_demographics.hd_vehicle_count<=3+2))
     and impala_tpcds_store.s_store_name = 'ese') s6,
 (select count(*) h11_30_to_12
 from impala_tpcds_store_sales, impala_tpcds_household_demographics , impala_tpcds_time_dim, impala_tpcds_store
 where ss_sold_time_sk = impala_tpcds_time_dim.t_time_sk
     and ss_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and impala_tpcds_time_dim.t_hour = 11
     and impala_tpcds_time_dim.t_minute >= 30
     and ((impala_tpcds_household_demographics.hd_dep_count = 2 and impala_tpcds_household_demographics.hd_vehicle_count<=2+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 4 and impala_tpcds_household_demographics.hd_vehicle_count<=4+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 3 and impala_tpcds_household_demographics.hd_vehicle_count<=3+2))
     and impala_tpcds_store.s_store_name = 'ese') s7,
 (select count(*) h12_to_12_30
 from impala_tpcds_store_sales, impala_tpcds_household_demographics , impala_tpcds_time_dim, impala_tpcds_store
 where ss_sold_time_sk = impala_tpcds_time_dim.t_time_sk
     and ss_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and impala_tpcds_time_dim.t_hour = 12
     and impala_tpcds_time_dim.t_minute < 30
     and ((impala_tpcds_household_demographics.hd_dep_count = 2 and impala_tpcds_household_demographics.hd_vehicle_count<=2+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 4 and impala_tpcds_household_demographics.hd_vehicle_count<=4+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 3 and impala_tpcds_household_demographics.hd_vehicle_count<=3+2))
     and impala_tpcds_store.s_store_name = 'ese') s8
;

explain select  *
from
 (select count(*) h8_30_to_9
 from impala_tpcds_store_sales, impala_tpcds_household_demographics , impala_tpcds_time_dim, impala_tpcds_store
 where ss_sold_time_sk = impala_tpcds_time_dim.t_time_sk   
     and ss_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk 
     and ss_store_sk = s_store_sk
     and impala_tpcds_time_dim.t_hour = 8
     and impala_tpcds_time_dim.t_minute >= 30
     and ((impala_tpcds_household_demographics.hd_dep_count = 2 and impala_tpcds_household_demographics.hd_vehicle_count<=2+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 4 and impala_tpcds_household_demographics.hd_vehicle_count<=4+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 3 and impala_tpcds_household_demographics.hd_vehicle_count<=3+2)) 
     and impala_tpcds_store.s_store_name = 'ese') s1,
 (select count(*) h9_to_9_30 
 from impala_tpcds_store_sales, impala_tpcds_household_demographics , impala_tpcds_time_dim, impala_tpcds_store
 where ss_sold_time_sk = impala_tpcds_time_dim.t_time_sk
     and ss_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk 
     and impala_tpcds_time_dim.t_hour = 9 
     and impala_tpcds_time_dim.t_minute < 30
     and ((impala_tpcds_household_demographics.hd_dep_count = 2 and impala_tpcds_household_demographics.hd_vehicle_count<=2+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 4 and impala_tpcds_household_demographics.hd_vehicle_count<=4+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 3 and impala_tpcds_household_demographics.hd_vehicle_count<=3+2))
     and impala_tpcds_store.s_store_name = 'ese') s2,
 (select count(*) h9_30_to_10 
 from impala_tpcds_store_sales, impala_tpcds_household_demographics , impala_tpcds_time_dim, impala_tpcds_store
 where ss_sold_time_sk = impala_tpcds_time_dim.t_time_sk
     and ss_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and impala_tpcds_time_dim.t_hour = 9
     and impala_tpcds_time_dim.t_minute >= 30
     and ((impala_tpcds_household_demographics.hd_dep_count = 2 and impala_tpcds_household_demographics.hd_vehicle_count<=2+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 4 and impala_tpcds_household_demographics.hd_vehicle_count<=4+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 3 and impala_tpcds_household_demographics.hd_vehicle_count<=3+2))
     and impala_tpcds_store.s_store_name = 'ese') s3,
 (select count(*) h10_to_10_30
 from impala_tpcds_store_sales, impala_tpcds_household_demographics , impala_tpcds_time_dim, impala_tpcds_store
 where ss_sold_time_sk = impala_tpcds_time_dim.t_time_sk
     and ss_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and impala_tpcds_time_dim.t_hour = 10 
     and impala_tpcds_time_dim.t_minute < 30
     and ((impala_tpcds_household_demographics.hd_dep_count = 2 and impala_tpcds_household_demographics.hd_vehicle_count<=2+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 4 and impala_tpcds_household_demographics.hd_vehicle_count<=4+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 3 and impala_tpcds_household_demographics.hd_vehicle_count<=3+2))
     and impala_tpcds_store.s_store_name = 'ese') s4,
 (select count(*) h10_30_to_11
 from impala_tpcds_store_sales, impala_tpcds_household_demographics , impala_tpcds_time_dim, impala_tpcds_store
 where ss_sold_time_sk = impala_tpcds_time_dim.t_time_sk
     and ss_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and impala_tpcds_time_dim.t_hour = 10 
     and impala_tpcds_time_dim.t_minute >= 30
     and ((impala_tpcds_household_demographics.hd_dep_count = 2 and impala_tpcds_household_demographics.hd_vehicle_count<=2+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 4 and impala_tpcds_household_demographics.hd_vehicle_count<=4+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 3 and impala_tpcds_household_demographics.hd_vehicle_count<=3+2))
     and impala_tpcds_store.s_store_name = 'ese') s5,
 (select count(*) h11_to_11_30
 from impala_tpcds_store_sales, impala_tpcds_household_demographics , impala_tpcds_time_dim, impala_tpcds_store
 where ss_sold_time_sk = impala_tpcds_time_dim.t_time_sk
     and ss_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk 
     and impala_tpcds_time_dim.t_hour = 11
     and impala_tpcds_time_dim.t_minute < 30
     and ((impala_tpcds_household_demographics.hd_dep_count = 2 and impala_tpcds_household_demographics.hd_vehicle_count<=2+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 4 and impala_tpcds_household_demographics.hd_vehicle_count<=4+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 3 and impala_tpcds_household_demographics.hd_vehicle_count<=3+2))
     and impala_tpcds_store.s_store_name = 'ese') s6,
 (select count(*) h11_30_to_12
 from impala_tpcds_store_sales, impala_tpcds_household_demographics , impala_tpcds_time_dim, impala_tpcds_store
 where ss_sold_time_sk = impala_tpcds_time_dim.t_time_sk
     and ss_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and impala_tpcds_time_dim.t_hour = 11
     and impala_tpcds_time_dim.t_minute >= 30
     and ((impala_tpcds_household_demographics.hd_dep_count = 2 and impala_tpcds_household_demographics.hd_vehicle_count<=2+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 4 and impala_tpcds_household_demographics.hd_vehicle_count<=4+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 3 and impala_tpcds_household_demographics.hd_vehicle_count<=3+2))
     and impala_tpcds_store.s_store_name = 'ese') s7,
 (select count(*) h12_to_12_30
 from impala_tpcds_store_sales, impala_tpcds_household_demographics , impala_tpcds_time_dim, impala_tpcds_store
 where ss_sold_time_sk = impala_tpcds_time_dim.t_time_sk
     and ss_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and impala_tpcds_time_dim.t_hour = 12
     and impala_tpcds_time_dim.t_minute < 30
     and ((impala_tpcds_household_demographics.hd_dep_count = 2 and impala_tpcds_household_demographics.hd_vehicle_count<=2+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 4 and impala_tpcds_household_demographics.hd_vehicle_count<=4+2) or
          (impala_tpcds_household_demographics.hd_dep_count = 3 and impala_tpcds_household_demographics.hd_vehicle_count<=3+2))
     and impala_tpcds_store.s_store_name = 'ese') s8
;
