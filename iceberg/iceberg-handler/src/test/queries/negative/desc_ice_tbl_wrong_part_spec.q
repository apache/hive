DROP TABLE IF EXISTS ice_t;

CREATE EXTERNAL TABLE ice_t (
  a INT,
  b STRING
)
PARTITIONED BY (
  c INT,
  d STRING
)
WRITE LOCALLY
ORDERED BY a DESC
STORED BY ICEBERG;

ALTER TABLE ice_t
SET PARTITION SPEC (
  c,
  d,
  truncate(2, b)
);

INSERT INTO TABLE ice_t
VALUES (5, "hello5", 6, "hello6");

-- this spec will resolve "bello5" to "be" due to truncate(2, b) which don't correspond to any partition available of size 3
DESC FORMATTED ice_t
PARTITION (c = 6, d = "hello6", b = "bello5");