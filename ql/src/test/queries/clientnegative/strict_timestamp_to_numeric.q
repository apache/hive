set hive.strict.checks.type.safety=true;
select cast(cast('2011-11-11' as date) as integer);
