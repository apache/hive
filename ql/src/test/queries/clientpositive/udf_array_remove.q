--! qt:dataset:src

-- SORT_QUERY_RESULTS

set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION array_remove;
DESCRIBE FUNCTION EXTENDED array_remove;

-- evaluates function for array of primitives
SELECT array_remove(array(1, 2, 3, null,3,4), 3);

SELECT array_remove(array(1.12, 2.23, 3.34, null,1.11,1.12,2.9),1.12);

SELECT array(1,2,3),array_remove(array(1, 2, 3),3);

SELECT array(1,2,3),array_remove(array(1, 2, 3),5);

SELECT array_remove(array(1.1234567890, 2.234567890, 3.34567890, null, 3.3456789, 2.234567,1.1234567890),1.1234567890);

SELECT array_remove(array(11234567890, 2234567890, 334567890, null, 11234567890, 2234567890, 334567890, null),11234567890);

SELECT array_remove(array(array("a","b","c","d"),array("a","b","c","d"),array("a","b","c","d","e"),null,array("e","a","b","c","d")),array("a","b","c","d"));

# handle null array cases

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/test_null_array;

dfs -copyFromLocal ../../data/files/test_null_array.csv ${system:test.tmp.dir}/test_null_array/;

create external table test_null_array (id string, value Array<String>) ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ':' collection items terminated by ',' location '${system:test.tmp.dir}/test_null_array';

select id,value from test_null_array;

select array_remove(value,id) from test_null_array;

select value, array_remove(value,id) from test_null_array;

dfs -rm -r ${system:test.tmp.dir}/test_null_array;