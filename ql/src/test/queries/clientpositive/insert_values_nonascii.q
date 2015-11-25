set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


create table insert_values_nonascii(t1 char(32), t2 string);

insert into insert_values_nonascii values("Абвгде Garçu 谢谢",  "Kôkaku ありがとう"), ("ございます", "kidôtai한국어");

select * from insert_values_nonascii;
