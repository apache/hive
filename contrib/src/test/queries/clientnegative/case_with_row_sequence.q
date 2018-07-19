--! qt:dataset:src
set hive.exec.submitviachild=true;
set hive.exec.submit.local.task.via.child=true;

drop temporary function row_sequence;

add jar ${system:maven.local.repository}/org/apache/hive/hive-contrib/${system:hive.version}/hive-contrib-${system:hive.version}.jar;
create temporary function row_sequence as 
'org.apache.hadoop.hive.contrib.udf.UDFRowSequence';

-- make sure a stateful function inside of CASE throws an exception
-- since the short-circuiting requirements are contradictory
SELECT CASE WHEN 3 > 2 THEN 10 WHEN row_sequence() > 5 THEN 20 ELSE 30 END
FROM src LIMIT 1;
