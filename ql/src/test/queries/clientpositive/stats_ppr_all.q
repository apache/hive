set hive.mapred.mode=nonstrict;
set hive.stats.fetch.column.stats=true;

drop table ss;

CREATE TABLE ss (
    sales_order_id  BIGINT,
    order_amount    FLOAT)
PARTITIONED BY (country STRING, year INT, month INT, day INT) stored as orc;

insert into ss partition(country="US", year=2015, month=1, day=1) values(1,22.0);
insert into ss partition(country="US", year=2015, month=2, day=1) values(2,2.0);
insert into ss partition(country="US", year=2015, month=1, day=2) values(1,2.0);

ANALYZE TABLE ss PARTITION(country,year,month,day) compute statistics for columns;

explain select sum(order_amount) from ss where (country="US" and year=2015 and month=2 and day=1);

explain select sum(order_amount) from ss where (year*10000+month*100+day) = "2015010" and 1>0;

explain select sum(order_amount) from ss where (year*100+month*10+day) = "201511" and 1>0;

explain select sum(order_amount) from ss where (year*100+month*10+day) > "201511" and 1>0;

explain select '1' from ss where (year*100+month*10+day) > "201511";