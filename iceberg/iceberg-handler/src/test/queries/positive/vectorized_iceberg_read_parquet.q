set hive.vectorized.execution.enabled=true;

drop table if exists tbl_ice_parquet;
create external table tbl_ice_parquet(a int, b string) stored by iceberg stored as parquet;
insert into table tbl_ice_parquet values (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five'), (111, 'one'), (22, 'two'), (11, 'one'), (44444, 'four'), (44, 'four');
analyze table tbl_ice_parquet compute statistics for columns;

explain select b, max(a) from tbl_ice_parquet group by b;
select b, max(a) from tbl_ice_parquet group by b;

create external table tbl_ice_parquet_all_types (
    t_float FLOAT,
    t_double DOUBLE,
    t_boolean BOOLEAN,
    t_int INT,
    t_bigint BIGINT,
    t_binary BINARY,
    t_string STRING,
    t_timestamp TIMESTAMP,
    t_date DATE,
    t_decimal DECIMAL(4,2)
    ) stored by iceberg stored as parquet;

insert into tbl_ice_parquet_all_types values (1.1, 1.2, false, 4, 567890123456789, '6', "col7", cast('2012-10-03 19:58:08' as timestamp), date('1234-09-09'), cast('10.01' as decimal(4,2)));

explain select max(t_float), t_double, t_boolean, t_int, t_bigint, t_binary, t_string, t_timestamp, t_date, t_decimal from tbl_ice_parquet_all_types
    group by t_double, t_boolean, t_int, t_bigint, t_binary, t_string, t_timestamp, t_date, t_decimal;
select max(t_float), t_double, t_boolean, t_int, t_bigint, t_binary, t_string, t_timestamp, t_date, t_decimal from tbl_ice_parquet_all_types
        group by t_double, t_boolean, t_int, t_bigint, t_binary, t_string, t_timestamp, t_date, t_decimal;

create external table tbl_ice_parquet_parted (
    a int,
    b string
    ) partitioned by (p1 string, p2 string)
    stored by iceberg stored as parquet location 'file:/tmp/tbl_ice_parquet_parted';

insert into tbl_ice_parquet_parted values
                                      (1, 'aa', 'Europe', 'Hungary'),
                                      (1, 'bb', 'Europe', 'Hungary'),
                                      (2, 'aa', 'America', 'USA'),
                                      (2, 'bb', 'America', 'Canada');
-- query with projection of partition columns' subset
select p1, a, min(b) from tbl_ice_parquet_parted group by p1, a;

-- move partition columns
alter table tbl_ice_parquet_parted change column p1 p1 string after a;

-- should yield to the same result as previously
select p1, a, min(b) from tbl_ice_parquet_parted group by p1, a;

-- move non-partition columns
alter table tbl_ice_parquet_parted change column a a int after b;

describe tbl_ice_parquet_parted;

-- should yield to the same result as previously
select p1, a, min(b) from tbl_ice_parquet_parted group by p1, a;

insert into tbl_ice_parquet_parted values ('Europe', 'cc', 3, 'Austria');

-- projecting all columns
select p1, p2, a, min(b) from tbl_ice_parquet_parted group by p1, p2, a;

drop table tbl_ice_parquet;
drop table tbl_ice_parquet_all_types;
drop table tbl_ice_parquet_parted;