set hive.llap.io.enabled=true;
set hive.vectorized.execution.enabled=true;
set hive.optimize.shared.work.merge.ts.schema=true;

create table iceberg_orc_compaction (a int, b int, c string) partitioned by (d int) stored by iceberg stored as orc;

insert into iceberg_orc_compaction values  (1, 11, "text1", 111),(2,22,"text2",222);
insert into iceberg_orc_compaction values  (3, 33, "text3", 333),(4,44,"text4",444);
insert into iceberg_orc_compaction values  (5, 55, "text5", 555),(6,66,"text6",666);

alter table iceberg_orc_compaction COMPACT 'major' and wait where c in ('text1', 'text2');
