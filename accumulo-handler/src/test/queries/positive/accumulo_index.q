DROP TABLE accumulo_index_test;

CREATE TABLE accumulo_index_test (
   rowid string,
   active boolean,
   num_offices tinyint,
   num_personel smallint,
   total_manhours int,
   num_shareholders bigint,
   eff_rating float,
   err_rating double,
   yearly_production decimal,
   start_date date,
   address varchar(100),
   phone char(13),
   last_update timestamp )
ROW FORMAT SERDE 'org.apache.hadoop.hive.accumulo.serde.AccumuloSerDe'
STORED BY 'org.apache.hadoop.hive.accumulo.AccumuloStorageHandler'
WITH SERDEPROPERTIES (
   "accumulo.columns.mapping" = ":rowID,a:act,a:off,a:per,a:mhs,a:shs,a:eff,a:err,a:yp,a:sd,a:addr,a:ph,a:lu",
   "accumulo.table.name"="accumulo_index_test",
   "accumulo.indexed.columns"="*",
   "accumulo.indextable.name"="accumulo_index_idx"
 );


insert into accumulo_index_test values( "row1", true, 55, 107, 555555, 1223232332,
                                 4.5, 0.8, 1232223, "2001-10-10", "123 main street",
                                 "555-555-5555", "2016-02-22 12:45:07.000000000");

select * from accumulo_index_test where active = 'true';
select * from accumulo_index_test where num_offices = 55;
select * from accumulo_index_test where num_personel = 107;
select * from accumulo_index_test where total_manhours < 555556;
select * from accumulo_index_test where num_shareholders >= 1223232331;
select * from accumulo_index_test where eff_rating <= 4.5;
select * from accumulo_index_test where err_rating >= 0.8;
select * from accumulo_index_test where yearly_production = 1232223;
select * from accumulo_index_test where start_date = "2001-10-10";
select * from accumulo_index_test where address >= "100 main street";
select * from accumulo_index_test where phone <= "555-555-5555";
select * from accumulo_index_test where last_update >= "2016-02-22 12:45:07";

DROP TABLE accumulo_index_test;
