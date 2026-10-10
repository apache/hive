CREATE TABLE zorder_test (
    ts timestamp,
    id int,
    data string)
WRITE LOCALLY ORDERED BY zorder(ts, id)
STORED BY iceberg STORED AS parquet;

INSERT INTO zorder_test VALUES
  (TIMESTAMP '2023-10-01 12:30:45.123999', 100, 'payload_A'),
  (TIMESTAMP '2023-10-01 12:30:45.123111', 200, 'payload_B'),
  (TIMESTAMP '2023-10-01 12:30:45.123555', 300, 'payload_C'),
  (TIMESTAMP '2023-10-01 12:30:45.123000', 400, 'payload_D'),
  (TIMESTAMP '2023-10-01 12:30:45.123999', 50,  'payload_E');

SELECT * FROM zorder_test;
DROP TABLE zorder_test;
