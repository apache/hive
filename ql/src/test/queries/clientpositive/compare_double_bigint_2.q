set hive.strict.checks.bucketing=false;

reset hive.mapred.mode;
set hive.strict.checks.type.safety=true;

-- This should fail until we fix the issue with precision when casting a bigint to a double

select * from src where cast(1 as bigint) = cast(1.0 as double) limit 10;
