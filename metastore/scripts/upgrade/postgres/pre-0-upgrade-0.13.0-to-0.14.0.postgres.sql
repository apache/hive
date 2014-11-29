
CREATE TABLE "PART_COL_STATS" (
 "CS_ID" bigint NOT NULL,
 "DB_NAME" character varying(128) DEFAULT NULL::character varying,
 "TABLE_NAME" character varying(128) DEFAULT NULL::character varying,
 "PARTITION_NAME" character varying(767) DEFAULT NULL::character varying,
 "COLUMN_NAME" character varying(128) DEFAULT NULL::character varying,
 "COLUMN_TYPE" character varying(128) DEFAULT NULL::character varying,
 "PART_ID" bigint NOT NULL,
 "LONG_LOW_VALUE" bigint,
 "LONG_HIGH_VALUE" bigint,
 "DOUBLE_LOW_VALUE" double precision,
 "DOUBLE_HIGH_VALUE" double precision,
 "BIG_DECIMAL_LOW_VALUE" character varying(4000) DEFAULT NULL::character varying,
 "BIG_DECIMAL_HIGH_VALUE" character varying(4000) DEFAULT NULL::character varying,
 "NUM_NULLS" bigint NOT NULL,
 "NUM_DISTINCTS" bigint,
 "AVG_COL_LEN" double precision,
 "MAX_COL_LEN" bigint,
 "NUM_TRUES" bigint,
 "NUM_FALSES" bigint,
 "LAST_ANALYZED" bigint NOT NULL
);
