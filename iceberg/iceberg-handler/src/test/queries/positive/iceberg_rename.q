
-- create a v1 table
create table icev1 (id int, name string) Stored by Iceberg;

-- insert some values
insert into icev1 values (1, 'SSD'),(2, 'RAM');

select * from icev1 order by id;

-- do the rename
explain alter table icev1 rename to icev1renamed;

alter table icev1 rename to icev1renamed;

select * from icev1renamed order by id;

-- create an unpartitioned v2 table
create table iceorgin (id int, name string) Stored by Iceberg TBLPROPERTIES ('format-version'='2');

-- insert some values
insert into iceorgin values (1, 'ABC'),(2, 'CBS'),(3, null),(4, 'POPI'),(5, 'AQWR'),(6, 'POIU'),
(9, null),(8,'POIKL'),(10, 'YUIO');

-- do some deletes
delete from iceorgin where id>9 OR id=8;

select * from iceorgin order by id;

-- do the rename

alter table iceorgin rename to icerenamed;

select * from icerenamed order by id;

-- create a partitioned v2 table
create table iceorginpart (id int) partitioned by (part string) Stored by Iceberg TBLPROPERTIES ('format-version'='2');

insert into iceorginpart values (1, 'ABC'),(2, 'CBS'),(3,'CBS'),(4, 'ABC'),(5, 'AQWR'),(6, 'ABC'),
(9, 'AQWR'),(8,'ABC'),(10, 'YUIO');

-- do some deletes
delete from iceorginpart where id<3 OR id=7;

select * from iceorginpart order by id;

explain alter table iceorginpart rename to icerenamedpart;

alter table iceorginpart rename to icerenamedpart;

select * from icerenamedpart order by id;

-- create a new unpartitioned table with old name
create table iceorgin (id int, name string) Stored by Iceberg TBLPROPERTIES ('format-version'='2');

insert into iceorgin values (100, 'ABCDWC');

select * from iceorgin order by id;

-- create a new partitioned table with old name

create table iceorginpart (id int) partitioned by (part string) Stored by Iceberg TBLPROPERTIES ('format-version'='2');

insert into iceorginpart values (22, 'DER'),(2, 'KLM');

select * from iceorginpart order by id;
