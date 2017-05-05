
drop table customer_address;

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

load data local inpath '../../data/files/customer_address.txt' overwrite into table customer_address;
analyze table customer_address compute statistics;
analyze table customer_address compute statistics for columns ca_state, ca_zip;

set hive.stats.fetch.column.stats=true;

set hive.stats.correlated.multi.key.joins=false;
explain select count(*) from customer_address a join customer_address b on (a.ca_zip = b.ca_zip and a.ca_state = b.ca_state);

set hive.stats.correlated.multi.key.joins=true;
explain select count(*) from customer_address a join customer_address b on (a.ca_zip = b.ca_zip and a.ca_state = b.ca_state);

drop table customer_address;
