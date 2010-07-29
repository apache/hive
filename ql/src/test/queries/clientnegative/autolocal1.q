set mapred.job.tracker=abracadabra;
set hive.exec.mode.local.auto.inputbytes.max=1;
set hive.exec.mode.local.auto=true;

SELECT key FROM src; 
