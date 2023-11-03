--! qt:replace:/^(?!LOCATION|.*HOOK).*metadata_table_test1/### TABLE DIRECTORY ###/

set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;
set hive.create.as.external.legacy=true;

CREATE TABLE metadata_test1(
    emp_number int,
    emp_name string,
    city string)
PARTITIONED BY(state string)
LOCATION 'pfile://${system:test.tmp.dir}/metadata_table_test1';

DESC FORMATTED metadata_test1;

INSERT INTO metadata_test1 PARTITION(state='A') VALUES (11, 'ABC', 'AA');
INSERT INTO metadata_test1 PARTITION(state='B') VALUES (12, 'XYZ', 'BX'), (13, 'UVW', 'BU');
select * from default.metadata_test1;
select "======================== list table directory =========================";
dfs -ls  ${system:test.tmp.dir}/metadata_table_test1/;

ALTER TABLE default.metadata_test1 PARTITION (state='A') RENAME TO PARTITION (state='C');
select * from metadata_test1;
select "======================== list table directory =========================";
dfs -ls  ${system:test.tmp.dir}/metadata_table_test1/;

drop table metadata_test1;
