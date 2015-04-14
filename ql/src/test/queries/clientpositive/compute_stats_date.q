
create table tab_date (
  origin_city_name string,
  dest_city_name string,
  fl_date date,
  arr_delay float,
  fl_num int
);

-- insert some data
load data local inpath '../../data/files/flights_join.txt' overwrite into table tab_date;

select count(*) from tab_date;

-- compute statistical summary of data
select compute_stats(fl_date, 16) from tab_date;

explain
analyze table tab_date compute statistics for columns fl_date;

analyze table tab_date compute statistics for columns fl_date;

describe formatted tab_date fl_date;

-- Update stats manually. Try both yyyy-mm-dd and integer value for high/low value
alter table tab_date update statistics for column fl_date set ('numDVs'='19', 'highValue'='2015-01-01', 'lowValue'='0');

describe formatted tab_date fl_date;
