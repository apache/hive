CREATE TABLE claims(claim_rec_id bigint, claim_invoice_num string, typ_c int);
ALTER TABLE claims UPDATE STATISTICS set ('numRows'='1154941534','rawDataSize'='1135307527922');


SET hive.stats.estimate=false;

EXPLAIN EXTENDED SELECT count(1) FROM claims WHERE typ_c=3;

SET hive.stats.ndv.estimate.percent=5e-7;

EXPLAIN EXTENDED SELECT count(1) FROM claims WHERE typ_c=3;
