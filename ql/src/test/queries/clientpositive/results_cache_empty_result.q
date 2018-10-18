--! qt:dataset:src

set hive.query.results.cache.enabled=true;
set hive.query.results.cache.nontransactional.tables.enabled=true;

explain
select count(*), key from src a where key < 0 group by key;
select count(*), key from src a where key < 0 group by key;

set test.comment="Cache should be used for this query";
set test.comment;
explain
select count(*), key from src a where key < 0 group by key;
select count(*), key from src a where key < 0 group by key;

