set hive.vectorized.execution.enabled=true;

drop table if exists tbl_ice_orc;
create external table tbl_ice_orc(a int, b string) stored by iceberg stored as orc;
insert into table tbl_ice_orc values (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five'), (111, 'one'), (22, 'two'), (11, 'one'), (44444, 'four'), (44, 'four');
analyze table tbl_ice_orc compute statistics for columns;

explain select b, max(a) from tbl_ice_orc group by b;
select b, max(a) from tbl_ice_orc group by b;



create external table tbl_ice_orc_all_types (
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
    ) stored by iceberg stored as orc;

insert into tbl_ice_orc_all_types values (1.1, 1.2, false, 4, 567890123456789, '6', "col7", cast('2012-10-03 19:58:08' as timestamp), date('1234-09-09'), cast('10.01' as decimal(4,2)));

explain select max(t_float), t_double, t_boolean, t_int, t_bigint, t_binary, t_string, t_timestamp, t_date, t_decimal from tbl_ice_orc_all_types
    group by t_double, t_boolean, t_int, t_bigint, t_binary, t_string, t_timestamp, t_date, t_decimal;
select max(t_float), t_double, t_boolean, t_int, t_bigint, t_binary, t_string, t_timestamp, t_date, t_decimal from tbl_ice_orc_all_types
        group by t_double, t_boolean, t_int, t_bigint, t_binary, t_string, t_timestamp, t_date, t_decimal;

drop table tbl_ice_orc;
drop table tbl_ice_orc_all_types;