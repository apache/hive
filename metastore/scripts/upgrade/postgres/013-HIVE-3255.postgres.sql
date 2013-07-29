SELECT '< HIVE-3255 Storing delegation tokens in metastore >';

-- Table "MASTER_KEYS" for classes [org.apache.hadoop.hive.metastore.model.MMasterKey]
CREATE TABLE  "MASTER_KEYS"
(
    "KEY_ID" SERIAL,
    "MASTER_KEY" varchar(767) NULL,
    PRIMARY KEY ("KEY_ID")
);

-- Table "DELEGATION_TOKENS" for classes [org.apache.hadoop.hive.metastore.model.MDelegationToken]
CREATE TABLE  "DELEGATION_TOKENS"
(
    "TOKEN_IDENT" varchar(767) NOT NULL,
    "TOKEN" varchar(767) NULL,
    PRIMARY KEY ("TOKEN_IDENT")
);

