set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

explain extended
 select * from (select a.key as ak, a.value as av, b.key as bk, b.value as bv from src a join src1 b where a.key = '429' ) c;

 select * from (select a.key as ak, a.value as av, b.key as bk, b.value as bv from src a join src1 b where a.key = '429' ) c;
