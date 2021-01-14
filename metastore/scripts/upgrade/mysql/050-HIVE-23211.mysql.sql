-- Not updating possible NULL values, since if NULLs existing in this table, the upgrade should fail
ALTER TABLE TXN_COMPONENTS CHANGE TC_TXNID TC_TXNID bigint NOT NULL;
ALTER TABLE COMPLETED_TXN_COMPONENTS CHANGE CTC_TXNID CTC_TXNID bigint NOT NULL;