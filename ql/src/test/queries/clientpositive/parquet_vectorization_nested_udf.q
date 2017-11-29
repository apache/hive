SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

SELECT SUM(abs(ctinyint)) from alltypesparquet;

