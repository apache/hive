-- Table MASTER_KEYS for classes [org.apache.hadoop.hive.metastore.model.MMasterKey] 

CREATE TABLE MASTER_KEYS
(
    KEY_ID NUMBER (10) NOT NULL,
    MASTER_KEY VARCHAR2(767) NULL
);

-- Table DELEGATION_TOKENS for classes [org.apache.hadoop.hive.metastore.model.MDelegationToken]
CREATE TABLE DELEGATION_TOKENS
(
    TOKEN_IDENT VARCHAR2(767) NOT NULL,
    TOKEN VARCHAR2(767) NULL
);

