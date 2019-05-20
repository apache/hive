set hive.test.authz.sstd.hs2.mode=true;

explain kill query 'query_1244656';
explain kill query 'query_123456677' 'query_1238503495';

kill query 'query_1244656';
kill query 'query_123456677' 'query_1238503495';
