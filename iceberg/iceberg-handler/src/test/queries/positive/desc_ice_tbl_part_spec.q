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

INSERT INTO TABLE ice_t
VALUES (1, "hello1", 2, "hello2");

INSERT INTO TABLE ice_t
VALUES (3, "hello3", 4, "hello4");

INSERT INTO TABLE ice_t
VALUES (5, "hello5", 6, "hello6");

DESC EXTENDED ice_t
PARTITION (c = 6, d = "hello6");

DESC FORMATTED ice_t
PARTITION (c = 6, d = "hello6");

ALTER TABLE ice_t
SET PARTITION SPEC (
  c,
  d,
  truncate(2, b)
);

INSERT INTO TABLE ice_t
VALUES (7, "hello7", 8, "hello8");

ALTER TABLE ice_t
SET PARTITION SPEC (
  bucket(16, c),
  d,
  truncate(2, b)
);

INSERT INTO TABLE ice_t
VALUES (7, "hello7", 8, "hello8");

DESC FORMATTED ice_t
PARTITION (c = 8, d = "hello8", b = "hello7");

-- this will also generate a valid result as "hello10" will resolve to "he" due to truncate(2, b)
DESC FORMATTED ice_t
PARTITION (c = 8, d = "hello8", b = "hello10");

DESC FORMATTED ice_t
PARTITION (c = 4, d = "hello4");
