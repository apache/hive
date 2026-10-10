-- row mode hands the Iceberg virtual columns to the operators through the IOContext
set hive.vectorized.execution.enabled=false;

create table ice_vc (id int, p string) partitioned by spec (p) stored by iceberg stored as parquet
  tblproperties ('format-version'='3');

insert into ice_vc values (1, 'a'), (2, 'a'), (3, 'b');
insert into ice_vc values (4, 'b');

select id, p, ROW__POSITION, PARTITION__SPEC__ID, PARTITION__NAME, FILE__PATH is not null,
  ROW__LINEAGE__ID, LAST__UPDATED__SEQUENCE__NUMBER
from ice_vc order by id;

drop table ice_vc;
