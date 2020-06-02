
-- defining a schedule in the past should be allowed
create scheduled query ingest cron '0 0 0 1 * ? 2000' defined as select 1;

