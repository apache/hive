drop table if exists ice_t;
create external table ice_t (a int, b string) partitioned by (c int, d string) write locally ordered by a desc stored by iceberg;

insert into table ice_t values( 1, "hello1" ,2, "hello2");
insert into table ice_t values( 3, "hello3" ,4, "hello4");
insert into table ice_t values( 5, "hello5" ,6, "hello6");

desc extended ice_t PARTITION(c=6,d="hello6");
desc formatted ice_t PARTITION(c=6,d="hello6");