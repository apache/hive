set hive.stats.fetch.column.stats=true;

drop table store_sales_n0;
drop table store_n0;
drop table customer_address;

-- s_store_sk is PK, ss_store_sk is FK
-- ca_address_sk is PK, ss_addr_sk is FK

create table store_sales_n0
(
    ss_sold_date_sk           int,
    ss_sold_time_sk           int,
    ss_item_sk                int,
    ss_customer_sk            int,
    ss_cdemo_sk               int,
    ss_hdemo_sk               int,
    ss_addr_sk                int,
    ss_store_sk               int,
    ss_promo_sk               int,
    ss_ticket_number          int,
    ss_quantity               int,
    ss_wholesale_cost         float,
    ss_list_price             float,
    ss_sales_price            float,
    ss_ext_discount_amt       float,
    ss_ext_sales_price        float,
    ss_ext_wholesale_cost     float,
    ss_ext_list_price         float,
    ss_ext_tax                float,
    ss_coupon_amt             float,
    ss_net_paid               float,
    ss_net_paid_inc_tax       float,
    ss_net_profit             float
)
row format delimited fields terminated by '|';

create table store_n0
(
    s_store_sk                int,
    s_store_id                string,
    s_rec_start_date          string,
    s_rec_end_date            string,
    s_closed_date_sk          int,
    s_store_name              string,
    s_number_employees        int,
    s_floor_space             int,
    s_hours                   string,
    s_manager                 string,
    s_market_id               int,
    s_geography_class         string,
    s_market_desc             string,
    s_market_manager          string,
    s_division_id             int,
    s_division_name           string,
    s_company_id              int,
    s_company_name            string,
    s_street_number           string,
    s_street_name             string,
    s_street_type             string,
    s_suite_number            string,
    s_city                    string,
    s_county                  string,
    s_state                   string,
    s_zip                     string,
    s_country                 string,
    s_gmt_offset              float,
    s_tax_precentage          float
)
row format delimited fields terminated by '|';

create table store_bigint
(
    s_store_sk                bigint,
    s_store_id                string,
    s_rec_start_date          string,
    s_rec_end_date            string,
    s_closed_date_sk          int,
    s_store_name              string,
    s_number_employees        int,
    s_floor_space             int,
    s_hours                   string,
    s_manager                 string,
    s_market_id               int,
    s_geography_class         string,
    s_market_desc             string,
    s_market_manager          string,
    s_division_id             int,
    s_division_name           string,
    s_company_id              int,
    s_company_name            string,
    s_street_number           string,
    s_street_name             string,
    s_street_type             string,
    s_suite_number            string,
    s_city                    string,
    s_county                  string,
    s_state                   string,
    s_zip                     string,
    s_country                 string,
    s_gmt_offset              float,
    s_tax_precentage          float
)
row format delimited fields terminated by '|';

create table customer_address
(
    ca_address_sk             int,
    ca_address_id             string,
    ca_street_number          string,
    ca_street_name            string,
    ca_street_type            string,
    ca_suite_number           string,
    ca_city                   string,
    ca_county                 string,
    ca_state                  string,
    ca_zip                    string,
    ca_country                string,
    ca_gmt_offset             float,
    ca_location_type          string
)
row format delimited fields terminated by '|';

load data local inpath '../../data/files/store.txt' overwrite into table store_n0;
load data local inpath '../../data/files/store.txt' overwrite into table store_bigint;
load data local inpath '../../data/files/store_sales.txt' overwrite into table store_sales_n0;
load data local inpath '../../data/files/customer_address.txt' overwrite into table customer_address;

analyze table store_n0 compute statistics;
analyze table store_n0 compute statistics for columns s_store_sk, s_floor_space;
analyze table store_bigint compute statistics;
analyze table store_bigint compute statistics for columns s_store_sk, s_floor_space;
analyze table store_sales_n0 compute statistics;
analyze table store_sales_n0 compute statistics for columns ss_store_sk, ss_addr_sk, ss_quantity;
analyze table customer_address compute statistics;
analyze table customer_address compute statistics for columns ca_address_sk;

explain select s.s_store_sk from store_n0 s join store_sales_n0 ss on (s.s_store_sk = ss.ss_store_sk);

-- widening cast: inferred PK-FK, thus same row count as previous query
explain select s.s_store_sk from store_bigint s join store_sales_n0 ss on (s.s_store_sk = ss.ss_store_sk);

explain select s.s_store_sk from store_n0 s join store_sales_n0 ss on (s.s_store_sk = ss.ss_store_sk) where s.s_store_sk > 0;

explain select s.s_store_sk from store_n0 s join store_sales_n0 ss on (s.s_store_sk = ss.ss_store_sk) where s.s_company_id > 0 and ss.ss_quantity > 10;

explain select s.s_store_sk from store_n0 s join store_sales_n0 ss on (s.s_store_sk = ss.ss_store_sk) where s.s_floor_space > 0;

explain select s.s_store_sk from store_n0 s join store_sales_n0 ss on (s.s_store_sk = ss.ss_store_sk) where ss.ss_quantity > 10;

explain select s.s_store_sk from store_n0 s join store_sales_n0 ss on (s.s_store_sk = ss.ss_store_sk) join store_n0 s1 on (s1.s_store_sk = ss.ss_store_sk);

explain select s.s_store_sk from store_n0 s join store_sales_n0 ss on (s.s_store_sk = ss.ss_store_sk) join store_n0 s1 on (s1.s_store_sk = ss.ss_store_sk) where s.s_store_sk > 1000;

explain select s.s_store_sk from store_n0 s join store_sales_n0 ss on (s.s_store_sk = ss.ss_store_sk) join store_n0 s1 on (s1.s_store_sk = ss.ss_store_sk) where s.s_floor_space > 1000;

explain select s.s_store_sk from store_n0 s join store_sales_n0 ss on (s.s_store_sk = ss.ss_store_sk) join store_n0 s1 on (s1.s_store_sk = ss.ss_store_sk) where ss.ss_quantity > 10;

explain select s.s_store_sk from store_n0 s join store_sales_n0 ss on (s.s_store_sk = ss.ss_store_sk) join customer_address ca on (ca.ca_address_sk = ss.ss_addr_sk);

drop table store_sales_n0;
drop table store_n0;
drop table store_bigint;
drop table customer_address;
