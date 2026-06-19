--! qt:dataset:src

set hive.query.results.cache.enabled=true;
set hive.query.results.cache.nontransactional.tables.enabled=true;
set hive.query.results.cache.directory=pfile://${system:test.tmp.dir}/results_cache_diff_fs;
set test.comment=hive.exec.scratchdir is;
set hive.exec.scratchdir;

explain
select count(*) from src a join src b on (a.key = b.key);
select count(*) from src a join src b on (a.key = b.key);

set test.comment="Cache should be used for this query";
set test.comment;
explain
select count(*) from src a join src b on (a.key = b.key);
select count(*) from src a join src b on (a.key = b.key);

