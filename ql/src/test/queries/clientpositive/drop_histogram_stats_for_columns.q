set hive.stats.kll.enable=true;
set metastore.stats.fetch.bitvector=true;
set metastore.stats.fetch.kll=true;
set hive.stats.autogather=true;
set hive.stats.column.autogather=true;

CREATE TABLE test_stats (a string, b int, c double) STORED AS ORC;

insert into test_stats (a, b, c) values ("a", 2, 1.1);

describe formatted test_stats;
describe formatted test_stats a;
describe formatted test_stats b;
describe formatted test_stats c;

alter table test_stats drop statistics for columns;

describe formatted test_stats;
describe formatted test_stats a;
describe formatted test_stats b;
describe formatted test_stats c;
