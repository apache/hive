--! qt:dataset:src
set hive.mapred.mode=nonstrict;
-- see HIVE-4033 earlier a flag named hasVC was not initialized correctly in MapOperator.java, resulting in NPE for following query. order by and limit in the query is not relevant, problem would be evident even without those. They are there to keep .q.out file small and sorted.

-- SORT_QUERY_RESULTS

explain select t3.BLOCK__OFFSET__INSIDE__FILE,t3.key,t3.value from src t1 join src t2 on t1.key = t2.key join src t3 on t2.value = t3.value order by t3.BLOCK__OFFSET__INSIDE__FILE,t3.key,t3.value limit 3;

select t3.BLOCK__OFFSET__INSIDE__FILE,t3.key,t3.value from src t1 join src t2 on t1.key = t2.key join src t3 on t2.value = t3.value order by t3.BLOCK__OFFSET__INSIDE__FILE,t3.key,t3.value limit 3;

explain
select t2.BLOCK__OFFSET__INSIDE__FILE
from src t1 join src t2 on t1.key = t2.key where t1.key < 100 order by t2.BLOCK__OFFSET__INSIDE__FILE;

select t2.BLOCK__OFFSET__INSIDE__FILE
from src t1 join src t2 on t1.key = t2.key where t1.key < 100 order by t2.BLOCK__OFFSET__INSIDE__FILE;
