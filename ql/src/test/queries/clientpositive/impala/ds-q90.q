--! qt:dataset:impala_dataset

explain cbo select  cast(amc as decimal(15,4))/cast(pmc as decimal(15,4)) am_pm_ratio
 from ( select count(*) amc
       from impala_tpcds_web_sales, impala_tpcds_household_demographics , impala_tpcds_time_dim, impala_tpcds_web_page
       where ws_sold_time_sk = impala_tpcds_time_dim.t_time_sk
         and ws_ship_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk
         and ws_web_page_sk = impala_tpcds_web_page.wp_web_page_sk
         and impala_tpcds_time_dim.t_hour between 9 and 9+1
         and impala_tpcds_household_demographics.hd_dep_count = 3
         and impala_tpcds_web_page.wp_char_count between 5000 and 5200) at,
      ( select count(*) pmc
       from impala_tpcds_web_sales, impala_tpcds_household_demographics , impala_tpcds_time_dim, impala_tpcds_web_page
       where ws_sold_time_sk = impala_tpcds_time_dim.t_time_sk
         and ws_ship_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk
         and ws_web_page_sk = impala_tpcds_web_page.wp_web_page_sk
         and impala_tpcds_time_dim.t_hour between 16 and 16+1
         and impala_tpcds_household_demographics.hd_dep_count = 3
         and impala_tpcds_web_page.wp_char_count between 5000 and 5200) pt
 order by am_pm_ratio
 limit 100;

explain select  cast(amc as decimal(15,4))/cast(pmc as decimal(15,4)) am_pm_ratio
 from ( select count(*) amc
       from impala_tpcds_web_sales, impala_tpcds_household_demographics , impala_tpcds_time_dim, impala_tpcds_web_page
       where ws_sold_time_sk = impala_tpcds_time_dim.t_time_sk
         and ws_ship_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk
         and ws_web_page_sk = impala_tpcds_web_page.wp_web_page_sk
         and impala_tpcds_time_dim.t_hour between 9 and 9+1
         and impala_tpcds_household_demographics.hd_dep_count = 3
         and impala_tpcds_web_page.wp_char_count between 5000 and 5200) at,
      ( select count(*) pmc
       from impala_tpcds_web_sales, impala_tpcds_household_demographics , impala_tpcds_time_dim, impala_tpcds_web_page
       where ws_sold_time_sk = impala_tpcds_time_dim.t_time_sk
         and ws_ship_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk
         and ws_web_page_sk = impala_tpcds_web_page.wp_web_page_sk
         and impala_tpcds_time_dim.t_hour between 16 and 16+1
         and impala_tpcds_household_demographics.hd_dep_count = 3
         and impala_tpcds_web_page.wp_char_count between 5000 and 5200) pt
 order by am_pm_ratio
 limit 100;

