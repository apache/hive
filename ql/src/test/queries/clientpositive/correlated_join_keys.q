
drop table customer_address_n0;

create table customer_address_n0
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

load data local inpath '../../data/files/customer_address.txt' overwrite into table customer_address_n0;
analyze table customer_address_n0 compute statistics;
analyze table customer_address_n0 compute statistics for columns ca_state, ca_zip;

set hive.stats.fetch.column.stats=true;

set hive.stats.correlated.multi.key.joins=false;
explain select count(*) from customer_address_n0 a join customer_address_n0 b on (a.ca_zip = b.ca_zip and a.ca_state = b.ca_state);

set hive.stats.correlated.multi.key.joins=true;
explain select count(*) from customer_address_n0 a join customer_address_n0 b on (a.ca_zip = b.ca_zip and a.ca_state = b.ca_state);

set hive.stats.multi.key.joins.independence.factor=0;
explain select count(*) from customer_address_n0 a join customer_address_n0 b on (a.ca_zip = b.ca_zip and a.ca_state = b.ca_state);

-- k1 and k2 tie for the most distinct values and k1, the first, has the fewest on the right side: at factor 0, as with
-- the correlated estimate, the unmatched values are those of k1 alone (500 - 10, not 500 - 500)
create table cjk_left (k1 int, k2 int);
create table cjk_right (k1 int, k2 int);
alter table cjk_left update statistics set('numRows'='1000', 'rawDataSize'='8000');
alter table cjk_right update statistics set('numRows'='1000', 'rawDataSize'='8000');
alter table cjk_left update statistics for column k1 set('numDVs'='500', 'numNulls'='0', 'lowValue'='1', 'highValue'='500');
alter table cjk_left update statistics for column k2 set('numDVs'='500', 'numNulls'='0', 'lowValue'='1', 'highValue'='500');
alter table cjk_right update statistics for column k1 set('numDVs'='10', 'numNulls'='0', 'lowValue'='1', 'highValue'='10');
alter table cjk_right update statistics for column k2 set('numDVs'='500', 'numNulls'='0', 'lowValue'='1', 'highValue'='500');
explain select * from cjk_left l left join cjk_right r on l.k1 = r.k1 and l.k2 = r.k2;

drop table customer_address_n0;
drop table cjk_left;
drop table cjk_right;
