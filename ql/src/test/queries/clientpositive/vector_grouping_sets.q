set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

-- We are not expecting either query to vectorize due to use of pruneing grouping set id,
-- or use of GROUPING__ID virtual column.
create table store_txt
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
    s_gmt_offset              decimal(5,2),
    s_tax_precentage          decimal(5,2)                  
)
row format delimited fields terminated by '|' 
stored as textfile;

LOAD DATA LOCAL INPATH '../../data/files/store_200' OVERWRITE INTO TABLE store_txt;

create table store
stored as orc as
select * from store_txt;

explain
select s_store_id
 from store
 group by s_store_id with rollup;

select s_store_id
 from store
 group by s_store_id with rollup;

explain
select s_store_id, GROUPING__ID
 from store
 group by s_store_id with rollup;

select s_store_id, GROUPING__ID
 from store
 group by s_store_id with rollup;