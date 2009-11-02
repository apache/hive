DROP TABLE rcfileTableLazyDecompress;
CREATE table rcfileTableLazyDecompress (key STRING, value STRING) STORED AS RCFile;

FROM src
INSERT OVERWRITE TABLE rcfileTableLazyDecompress SELECT src.key, src.value LIMIT 10;

SELECT key, value FROM rcfileTableLazyDecompress where key > 238;

SELECT key, value FROM rcfileTableLazyDecompress where key > 238 and key < 400;

SELECT key, count(1) FROM rcfileTableLazyDecompress where key > 238 group by key;

set mapred.output.compress=true;
set hive.exec.compress.output=true;

FROM src
INSERT OVERWRITE TABLE rcfileTableLazyDecompress SELECT src.key, src.value LIMIT 10;

SELECT key, value FROM rcfileTableLazyDecompress where key > 238;

SELECT key, value FROM rcfileTableLazyDecompress where key > 238 and key < 400;

SELECT key, count(1) FROM rcfileTableLazyDecompress where key > 238 group by key;

set mapred.output.compress=false;
set hive.exec.compress.output=false;

DROP TABLE rcfileTableLazyDecompress;