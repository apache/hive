-- SORT_QUERY_RESULTS

explain extended
 select * from (select a.key as ak, a.value as av, b.key as bk, b.value as bv from src a join src1 b where a.key = '429' ) c;

 select * from (select a.key as ak, a.value as av, b.key as bk, b.value as bv from src a join src1 b where a.key = '429' ) c;

-- Test constant propagation where the column name is aliased to its original name, see HIVE-14705 for details

explain extended select * from (select key-1 as key from srcbucket where key = 6) z;
select * from (select key-1 as key from srcbucket where key = 6) z;

-- Test with multiple levels of sub-queries

explain extended select y.key-1 as key from (select z.key-1 as key from (select key-1 as key from srcbucket where key = 6) z) y;
select y.key1-1 from (select z.key-1 as key1 from (select key-1 as key from srcbucket where key = 6) z) y;
