CREATE TABLE encryptedWith128BitsKeyDB.encryptedTableIn128BitsKey(key STRING, value STRING);

INSERT OVERWRITE TABLE encryptedWith128BitsKeyDB.encryptedTableIn128BitsKey SELECT * FROM src;

SELECT * FROM encryptedWith128BitsKeyDB.encryptedTableIn128BitsKey;

EXPLAIN EXTENDED SELECT * FROM src t1 JOIN encryptedWith128BitsKeyDB.encryptedTableIn128BitsKey t2 WHERE t1.key = t2.key;