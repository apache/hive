set hive.vectorized.execution.enabled=true;

drop table if exists tbl_ice_orc;
create external table tbl_ice_orc(a int, b string) stored by iceberg stored as orc
TBLPROPERTIES ('iceberg.decimal64.vectorization'='true',"format-version"='1');
insert into table tbl_ice_orc values (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five'), (111, 'one'), (22, 'two'), (11, 'one'), (44444, 'four'), (44, 'four');
analyze table tbl_ice_orc compute statistics for columns;

explain select b, max(a) from tbl_ice_orc group by b;
select b, max(a) from tbl_ice_orc group by b;

explain vectorization only detail select b, max(a) from tbl_ice_orc group by b;

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
    ) stored by iceberg stored as orc
    TBLPROPERTIES ('iceberg.decimal64.vectorization'='true',"format-version"='1');
insert into tbl_ice_orc_all_types values (1.1, 1.2, false, 4, 567890123456789, '6', "col7", cast('2012-10-03 19:58:08' as timestamp), date('1234-09-09'), cast('10.01' as decimal(4,2)));

explain select max(t_float), t_double, t_boolean, t_int, t_bigint, t_binary, t_string, t_timestamp, t_date, t_decimal from tbl_ice_orc_all_types
    group by t_double, t_boolean, t_int, t_bigint, t_binary, t_string, t_timestamp, t_date, t_decimal;
select max(t_float), t_double, t_boolean, t_int, t_bigint, t_binary, t_string, t_timestamp, t_date, t_decimal from tbl_ice_orc_all_types
        group by t_double, t_boolean, t_int, t_bigint, t_binary, t_string, t_timestamp, t_date, t_decimal;

explain vectorization only detail select max(t_float), t_double, t_boolean, t_int, t_bigint, t_binary, t_string, t_timestamp, t_date, t_decimal from tbl_ice_orc_all_types
group by t_double, t_boolean, t_int, t_bigint, t_binary, t_string, t_timestamp, t_date, t_decimal;

create external table tbl_ice_orc_parted (
    a int,
    b string
    ) partitioned by (p1 string, p2 string)
    stored by iceberg stored as orc location 'file:/tmp/tbl_ice_orc_parted'
    TBLPROPERTIES ('iceberg.decimal64.vectorization'='true',"format-version"='1');

insert into tbl_ice_orc_parted values
                                      (1, 'aa', 'Europe', 'Hungary'),
                                      (1, 'bb', 'Europe', 'Hungary'),
                                      (2, 'aa', 'America', 'USA'),
                                      (2, 'bb', 'America', 'Canada');
-- query with projection of partition columns' subset
select p1, a, min(b) from tbl_ice_orc_parted group by p1, a;

-- move partition columns
alter table tbl_ice_orc_parted change column p1 p1 string after a;

-- should yield to the same result as previously
select p1, a, min(b) from tbl_ice_orc_parted group by p1, a;

explain vectorization only detail select p1, a, min(b) from tbl_ice_orc_parted group by p1, a;

-- move non-partition columns
alter table tbl_ice_orc_parted change column a a int after b;

describe tbl_ice_orc_parted;

-- should yield to the same result as previously
select p1, a, min(b) from tbl_ice_orc_parted group by p1, a;

explain vectorization only detail select p1, a, min(b) from tbl_ice_orc_parted group by p1, a;

insert into tbl_ice_orc_parted values ('Europe', 'cc', 3, 'Austria');

-- projecting all columns
select p1, p2, a, min(b) from tbl_ice_orc_parted group by p1, p2, a;

explain vectorization only detail select p1, p2, a, min(b) from tbl_ice_orc_parted group by p1, p2, a;

-- create iceberg table with complex types
create external table tbl_ice_orc_complex (
    a int,
    arrayofprimitives array<string>,
    arrayofarrays array<array<string>>,
    arrayofmaps array<map<string, string>>,
    arrayofstructs array<struct<something:string, someone:string, somewhere:string>>,
    mapofprimitives map<string, string>,
    mapofarrays map<string, array<string>>,
    mapofmaps map<string, map<string, string>>,
    mapofstructs map<string, struct<something:string, someone:string, somewhere:string>>,
    structofprimitives struct<something:string, somewhere:string>,
    structofarrays struct<names:array<string>, birthdays:array<string>>,
    structofmaps struct<map1:map<string, string>, map2:map<string, string>>
    ) stored by iceberg stored as orc
    TBLPROPERTIES ('iceberg.decimal64.vectorization'='true',"format-version"='1');

-- insert some test data
insert into tbl_ice_orc_complex values (
    1,
    array('a','b','c'),
    array(array('a'), array('b', 'c')),
    array(map('a','b'), map('e','f')),
    array(named_struct('something', 'a', 'someone', 'b', 'somewhere', 'c'), named_struct('something', 'e', 'someone', 'f', 'somewhere', 'g')),
    map('a', 'b'),
    map('a', array('b','c')),
    map('a', map('b','c')),
    map('a', named_struct('something', 'b', 'someone', 'c', 'somewhere', 'd')),
    named_struct('something', 'a', 'somewhere', 'b'),
    named_struct('names', array('a', 'b'), 'birthdays', array('c', 'd', 'e')),
    named_struct('map1', map('a', 'b'), 'map2', map('c', 'd')));

select * from tbl_ice_orc_complex order by a;

explain vectorization only detail select * from tbl_ice_orc_complex order by a;

drop table tbl_ice_orc;
drop table tbl_ice_orc_all_types;
drop table tbl_ice_orc_parted;
drop table tbl_ice_orc_complex;