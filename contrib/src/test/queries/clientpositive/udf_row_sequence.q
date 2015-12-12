-- The ORDER BY on the outer query will typically have no effect,
-- but there is really no guarantee that the ordering is preserved
-- across various SQL operators.

drop temporary function row_sequence;

add jar ${system:maven.local.repository}/org/apache/hive/hive-contrib/${system:hive.version}/hive-contrib-${system:hive.version}.jar;

create temporary function row_sequence as 
'org.apache.hadoop.hive.contrib.udf.UDFRowSequence';

DESCRIBE FUNCTION EXTENDED row_sequence;

set mapred.reduce.tasks=1;
set hive.mapred.mode=nonstrict;
explain
select key, row_sequence() as r
from (select key from src order by key) x
order by r;

select key, row_sequence() as r
from (select key from src order by key) x
order by r;

-- make sure stateful functions do not get short-circuited away
-- a true result for key=105 would indicate undesired short-circuiting
select key, (key = 105) and (row_sequence() = 1)
from (select key from src order by key) x
order by key limit 20;

drop temporary function row_sequence;
