drop table if exists ice_t;
create external table ice_t (a int, b string) partitioned by (c int, d string) write locally ordered by a desc stored by iceberg;

insert into table ice_t values( 1, "hello1" ,2, "hello2" );
insert into table ice_t values( 3, "hello3" ,4, "hello4" );
insert into table ice_t values( 5, "hello5" ,6, "hello6" );

desc extended ice_t PARTITION( c=6, d="hello6" );
desc formatted ice_t PARTITION( c=6, d="hello6" );

alter table ice_t set partition spec ( c, d, b );
insert into table ice_t values( 7, "hello7" , 8 , "hello8" );
desc formatted ice_t PARTITION( c=8 , d="hello8", b="hello7" );
desc formatted ice_t PARTITION( c=4 , d="hello4" );
