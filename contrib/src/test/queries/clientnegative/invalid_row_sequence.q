--! qt:dataset:src
-- Verify that a stateful UDF cannot be used outside of the SELECT list

drop temporary function row_sequence;

add jar ${system:maven.local.repository}/org/apache/hive/hive-contrib/${system:hive.version}/hive-contrib-${system:hive.version}.jar;

create temporary function row_sequence as 
'org.apache.hadoop.hive.contrib.udf.UDFRowSequence';
set hive.mapred.mode=nonstrict;
select key
from (select key from src order by key) x
where row_sequence() < 5
order by key;
