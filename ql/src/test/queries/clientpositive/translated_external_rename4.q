set metastore.metadata.transformer.class=org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer;
set metastore.metadata.transformer.location.mode=prohibit;

set hive.fetch.task.conversion=none;
set hive.compute.query.using.stats=false;

set hive.create.as.external.legacy=true;

CREATE TABLE part_test(
c1 string
,c2 string
)PARTITIONED BY (dat string);

insert into part_test values ("11","th","20220101");
insert into part_test values ("22","th","20220102");

alter table part_test rename to part_test11;


desc formatted part_test11;
desc formatted part_test11 partition(dat="20220101");
