--! qt:timezone:Europe/Paris
DROP TABLE IF EXISTS payments;
CREATE EXTERNAL TABLE payments (card string) PARTITIONED BY(txn_datetime TIMESTAMP) STORED AS ORC;
INSERT into payments VALUES('3333-4444-2222-9999', '2023-03-26 02:30:00'), ('3333-4444-2222-9999', '2023-03-26 03:30:00');
SELECT * FROM payments WHERE txn_datetime = '2023-03-26 02:30:00';
SELECT * FROM payments WHERE txn_datetime = '2023-03-26 03:30:00';
