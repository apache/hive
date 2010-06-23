-- The ORDER BY on the outer query will typically have no effect,
-- but there is really no guarantee that the ordering is preserved
-- across various SQL operators.

drop temporary function row_sequence;

add jar ../build/contrib/hive_contrib.jar;

create temporary function row_sequence as 
'org.apache.hadoop.hive.contrib.udf.UDFRowSequence';

DESCRIBE FUNCTION EXTENDED row_sequence;

set mapred.reduce.tasks=1;

explain
select key, row_sequence() as r
from (select key from src order by key) x
order by r;

select key, row_sequence() as r
from (select key from src order by key) x
order by r;

drop temporary function row_sequence;
