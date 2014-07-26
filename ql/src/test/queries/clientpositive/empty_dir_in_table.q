dfs ${system:test.dfs.mkdir} hdfs:///target/tmp/test_empty_table;

create external table roottable (key string) row format delimited fields terminated by '\\t' stored as textfile location 'hdfs:///target/tmp/test_empty_table';
select count(*) from roottable;

insert into table roottable select key from src where (key < 20) order by key;
select count(*) from roottable;

dfs ${system:test.dfs.mkdir} hdfs:///target/tmp/test_empty_table/empty;
select count(*) from roottable;