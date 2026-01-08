drop table if exists ice_t;
create external table ice_t (a int, b string) partitioned by (c int, d string) write locally ordered by a desc stored by iceberg;
insert into table ice_t values( 5, "hello5" ,6, "hello6");
desc formatted ice_t PARTITION(c=6, d="hello");