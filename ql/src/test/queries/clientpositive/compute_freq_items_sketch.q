--! qt:dataset:src
--! qt:dataset:alltypesorc

-- Note that ds_freq_sketch(..., maxSize) calls are not vectorized, independently of the value of
-- 'hive.vectorized.execution.enabled' because the current framework handles single param function calls only;
-- here we are adding the ds_kll_sketch(..., 200) version to be able to check if the two sketches are the same,
-- since the default k value is set to 200 in HiveConfig;
--
--set hive.vectorized.execution.enabled=false;
--select ds_freq_frequent_items(ds_freq_sketch(cstring1), 2) from alltypesorc;
set hive.cli.print.escape.crlf=false;
--set hive.metastore.partition.name.whitelist.pattern=metastore.stats.fetch.bitvector;
set metastore.stats.fetch.bitvector=true;
set hive.stats.freq.items.enable=true;

-- non-partitioned table (to check table statistics)
CREATE TABLE test (a string, b string);

INSERT INTO test VALUES ("a", '1');
INSERT INTO test VALUES ("b", '2');
INSERT INTO test VALUES ("b", '2');
INSERT INTO test VALUES ("b", '2');

INSERT INTO test VALUES ("c", '2');
INSERT INTO test VALUES ("c", '2');
INSERT INTO test VALUES ("c", '2');
INSERT INTO test VALUES ("c", '2');
INSERT INTO test VALUES ("c", '2');

INSERT INTO test VALUES ("d", '4');
INSERT INTO test VALUES ("d", '5');
INSERT INTO test VALUES ("d", '6');
INSERT INTO test VALUES ("e", '7');
INSERT INTO test VALUES ("e", '3');
INSERT INTO test VALUES ("e", '3');
INSERT INTO test VALUES ("e", '3');
INSERT INTO test VALUES ("abcdefghijklmnopqrstuvwxyz_12345", '1');

ANALYZE TABLE test COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE test COMPUTE STATISTICS;

DESCRIBE EXTENDED test;

DESCRIBE FORMATTED test a;

SELECT * from test where b = '2' and a = 'c';  -- cost = 0.14, 0.16 Vs w/sketches 0.47 / 0.29

EXPLAIN CBO COST SELECT * from test where b = '2' and a = 'c';


-- partitioned table (to check table statistics)
CREATE TABLE test2 (b string, c string) partitioned by (a string);
INSERT INTO test2 VALUES ('1',"A", "a" );
INSERT INTO test2 VALUES ('2',"B", "a" );
INSERT INTO test2 VALUES ('3',"C", "a" );

INSERT INTO test2 VALUES ('1',"B", "b" );

INSERT INTO test2 VALUES ('2',"B", "b" );
INSERT INTO test2 VALUES ('2',"B", "b" );
INSERT INTO test2 VALUES ('2',"B", "b" );
INSERT INTO test2 VALUES ('2',"I", "b" );
INSERT INTO test2 VALUES ('2',"I", "b" );
INSERT INTO test2 VALUES ('2',"I", "b" );
INSERT INTO test2 VALUES ('2',"I", "b" );

INSERT INTO test2 VALUES ('3',"I", "b" );
INSERT INTO test2 VALUES ('4',"I", "b" );
INSERT INTO test2 VALUES ('5',"I", "b" );
INSERT INTO test2 VALUES ('6',"I", "b" );
INSERT INTO test2 VALUES ('7',"I", "b" );

INSERT INTO test2 VALUES ('8',"I", "c" );
INSERT INTO test2 VALUES ('8',"I", "c" );
INSERT INTO test2 VALUES ('8',"J", "c" );
INSERT INTO test2 VALUES ('8',"K", "c" );

ANALYZE TABLE test2 COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE test2 COMPUTE STATISTICS;

DESCRIBE EXTENDED test2;

DESCRIBE FORMATTED test2 partition(a='a') c;

DESCRIBE FORMATTED test2 partition(a='c') c;

DESCRIBE FORMATTED test2 c;

SELECT * from test2 where b = '2' and c = 'B';              -- .1 / .142 w/sketches .36 / .22

EXPLAIN CBO COST SELECT * from test2 where b = '2' and c = 'B';


CREATE TABLE test3 (b string, c string) partitioned by (a string);
INSERT INTO test3 VALUES ('1',"A", "a" );
INSERT INTO test3 VALUES ('2',"B", "a" );
INSERT INTO test3 VALUES ('3',"C", "a" );
INSERT INTO test3 VALUES ('3',"D", "a" );
INSERT INTO test3 VALUES ('3',"E", "a" );
INSERT INTO test3 VALUES ('3',"F", "a" );
INSERT INTO test3 VALUES ('3',"G", "a" );
INSERT INTO test3 VALUES ('3',"H", "a" );
INSERT INTO test3 VALUES ('3',"I", "a" );
INSERT INTO test3 VALUES ('3',"J", "a" );
INSERT INTO test3 VALUES ('3',"K", "a" );
INSERT INTO test3 VALUES ('3',"L", "a" );
INSERT INTO test3 VALUES ('3',"M", "a" );
INSERT INTO test3 VALUES ('3',"N", "a" );
INSERT INTO test3 VALUES ('3',"O", "a" );
INSERT INTO test3 VALUES ('3',"P", "a" );
INSERT INTO test3 VALUES ('3',"Q", "a" );
INSERT INTO test3 VALUES ('3',"R", "a" );
INSERT INTO test3 VALUES ('3',"abcdefghijklmnopqrstuvwxyz_12345", "a" );
INSERT INTO test3 VALUES ('3',"abcdefghijklmnopqrstuvwxyz_12345", "a" );
INSERT INTO test3 VALUES ('3',"abcdefghijklmnopqrstuvwxyz_12345", "a" );
INSERT INTO test3 VALUES ('3',"abcdefghijklmnopqrstuvwxyz_12345", "a" );
INSERT INTO test3 VALUES ('3',"abcdefghijklmnopqrstuvwxyz_12345", "a" );
INSERT INTO test3 VALUES ('3',"abcdefghijklmnopqrstuvwxyz_12345", "a" );

ANALYZE TABLE test3 COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE test3 COMPUTE STATISTICS;

DESCRIBE EXTENDED test3;

DESCRIBE FORMATTED test3 partition(a='a') c;

DESCRIBE FORMATTED test3 c;
