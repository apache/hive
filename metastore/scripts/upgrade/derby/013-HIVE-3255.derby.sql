--
-- HIVE-3255 Storing Delegation tokens in Metastore
--

-- Table MASTER_KEYS for classes [org.apache.hadoop.hive.metastore.model.MMasterKey]
CREATE TABLE  MASTER_KEYS
(
    KEY_ID INTEGER NOT NULL generated always as identity (start with 1),
    MASTER_KEY VARCHAR(767)
);

-- Table DELEGATION_TOKENS for classes [org.apache.hadoop.hive.metastore.model.MDelegationToken]
CREATE TABLE  DELEGATION_TOKENS
(
    TOKEN_IDENT VARCHAR(767) NOT NULL,
    TOKEN VARCHAR(767)
);
