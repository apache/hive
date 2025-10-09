set hive.fetch.task.conversion = none;

create external table test_table(
  strcol string,
  intcol integer
) stored as ORC;

insert into table test_table values ('ABC', 1), ('DEF', 2);
select * from test_table;

-- split serialization can be sanity-checked from hive.log by HiveSplitGenerator messages
-- 10 bytes threshold must trigger split serialization to filesystem
set hive.tez.input.fs.serialization.threshold = 10;
select * from test_table;
