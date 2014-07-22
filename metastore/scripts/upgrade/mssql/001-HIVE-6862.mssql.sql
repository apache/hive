-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

SELECT 'Cumulative change to upgrade metastore schema from 0.12 to 0.13' AS MESSAGE;
------------------------------------------------------------------
-- DataNucleus SchemaTool (ran at 08/04/2014 14:59:28)
------------------------------------------------------------------
-- Schema diff for jdbc:sqlserver://172.16.65.141:1433;databaseName=master and the following classes:-
--     org.apache.hadoop.hive.metastore.model.MColumnDescriptor
--     org.apache.hadoop.hive.metastore.model.MDBPrivilege
--     org.apache.hadoop.hive.metastore.model.MDatabase
--     org.apache.hadoop.hive.metastore.model.MDelegationToken
--     org.apache.hadoop.hive.metastore.model.MFieldSchema
--     org.apache.hadoop.hive.metastore.model.MFunction
--     org.apache.hadoop.hive.metastore.model.MGlobalPrivilege
--     org.apache.hadoop.hive.metastore.model.MIndex
--     org.apache.hadoop.hive.metastore.model.MMasterKey
--     org.apache.hadoop.hive.metastore.model.MOrder
--     org.apache.hadoop.hive.metastore.model.MPartition
--     org.apache.hadoop.hive.metastore.model.MPartitionColumnPrivilege
--     org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics
--     org.apache.hadoop.hive.metastore.model.MPartitionEvent
--     org.apache.hadoop.hive.metastore.model.MPartitionPrivilege
--     org.apache.hadoop.hive.metastore.model.MResourceUri
--     org.apache.hadoop.hive.metastore.model.MRole
--     org.apache.hadoop.hive.metastore.model.MRoleMap
--     org.apache.hadoop.hive.metastore.model.MSerDeInfo
--     org.apache.hadoop.hive.metastore.model.MStorageDescriptor
--     org.apache.hadoop.hive.metastore.model.MStringList
--     org.apache.hadoop.hive.metastore.model.MTable
--     org.apache.hadoop.hive.metastore.model.MTableColumnPrivilege
--     org.apache.hadoop.hive.metastore.model.MTableColumnStatistics
--     org.apache.hadoop.hive.metastore.model.MTablePrivilege
--     org.apache.hadoop.hive.metastore.model.MType
--     org.apache.hadoop.hive.metastore.model.MVersionTable
--
-- Table MASTER_KEYS for classes [org.apache.hadoop.hive.metastore.model.MMasterKey]
-- Table IDXS for classes [org.apache.hadoop.hive.metastore.model.MIndex]
-- Table PART_COL_STATS for classes [org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics]
ALTER TABLE PART_COL_STATS ADD BIG_DECIMAL_HIGH_VALUE varchar(255) NULL;

ALTER TABLE PART_COL_STATS ADD BIG_DECIMAL_LOW_VALUE varchar(255) NULL;

-- Table PART_PRIVS for classes [org.apache.hadoop.hive.metastore.model.MPartitionPrivilege]
-- Table SKEWED_STRING_LIST for classes [org.apache.hadoop.hive.metastore.model.MStringList]
-- Table ROLES for classes [org.apache.hadoop.hive.metastore.model.MRole]
-- Table PARTITIONS for classes [org.apache.hadoop.hive.metastore.model.MPartition]
-- Table CDS for classes [org.apache.hadoop.hive.metastore.model.MColumnDescriptor]
-- Table VERSION for classes [org.apache.hadoop.hive.metastore.model.MVersionTable]
-- Table GLOBAL_PRIVS for classes [org.apache.hadoop.hive.metastore.model.MGlobalPrivilege]
-- Table PART_COL_PRIVS for classes [org.apache.hadoop.hive.metastore.model.MPartitionColumnPrivilege]
-- Table DB_PRIVS for classes [org.apache.hadoop.hive.metastore.model.MDBPrivilege]
-- Table TAB_COL_STATS for classes [org.apache.hadoop.hive.metastore.model.MTableColumnStatistics]
ALTER TABLE TAB_COL_STATS ADD BIG_DECIMAL_HIGH_VALUE varchar(255) NULL;

ALTER TABLE TAB_COL_STATS ADD BIG_DECIMAL_LOW_VALUE varchar(255) NULL;

-- Table TYPES for classes [org.apache.hadoop.hive.metastore.model.MType]
-- Table TBL_PRIVS for classes [org.apache.hadoop.hive.metastore.model.MTablePrivilege]
-- Table DBS for classes [org.apache.hadoop.hive.metastore.model.MDatabase]
ALTER TABLE DBS ADD OWNER_TYPE varchar(10) NULL;

ALTER TABLE DBS ADD OWNER_NAME varchar(128) NULL;

-- Table TBL_COL_PRIVS for classes [org.apache.hadoop.hive.metastore.model.MTableColumnPrivilege]
-- Table DELEGATION_TOKENS for classes [org.apache.hadoop.hive.metastore.model.MDelegationToken]
-- Table SERDES for classes [org.apache.hadoop.hive.metastore.model.MSerDeInfo]
-- Table FUNCS for classes [org.apache.hadoop.hive.metastore.model.MFunction]
CREATE TABLE FUNCS
(
    FUNC_ID bigint NOT NULL,
    CLASS_NAME varchar(4000) NULL,
    CREATE_TIME int NOT NULL,
    DB_ID bigint NULL,
    FUNC_NAME varchar(128) NULL,
    FUNC_TYPE int NOT NULL,
    OWNER_NAME varchar(128) NULL,
    OWNER_TYPE varchar(10) NULL
);

ALTER TABLE FUNCS ADD CONSTRAINT FUNCS_PK PRIMARY KEY (FUNC_ID);

-- Table ROLE_MAP for classes [org.apache.hadoop.hive.metastore.model.MRoleMap]
-- Table TBLS for classes [org.apache.hadoop.hive.metastore.model.MTable]
-- Table SDS for classes [org.apache.hadoop.hive.metastore.model.MStorageDescriptor]
-- Table PARTITION_EVENTS for classes [org.apache.hadoop.hive.metastore.model.MPartitionEvent]
-- Table COLUMNS_V2 for join relationship
-- Table TABLE_PARAMS for join relationship
-- Table PARTITION_KEYS for join relationship
-- Table SERDE_PARAMS for join relationship
-- Table SD_PARAMS for join relationship
-- Table FUNC_RU for join relationship
CREATE TABLE FUNC_RU
(
    FUNC_ID bigint NOT NULL,
    RESOURCE_TYPE int NOT NULL,
    RESOURCE_URI varchar(4000) NULL,
    INTEGER_IDX int NOT NULL
);

ALTER TABLE FUNC_RU ADD CONSTRAINT FUNC_RU_PK PRIMARY KEY (FUNC_ID,INTEGER_IDX);

-- Table PARTITION_PARAMS for join relationship
-- Table SORT_COLS for join relationship
-- Table SKEWED_COL_NAMES for join relationship
-- Table TYPE_FIELDS for join relationship
-- Table DATABASE_PARAMS for join relationship
-- Table INDEX_PARAMS for join relationship
-- Table BUCKETING_COLS for join relationship
-- Table PARTITION_KEY_VALS for join relationship
-- Table SKEWED_STRING_LIST_VALUES for join relationship
-- Table SKEWED_COL_VALUE_LOC_MAP for join relationship
-- Table SKEWED_VALUES for join relationship
-- Constraints for table MASTER_KEYS for class(es) [org.apache.hadoop.hive.metastore.model.MMasterKey]

-- Constraints for table IDXS for class(es) [org.apache.hadoop.hive.metastore.model.MIndex]

-- Constraints for table PART_COL_STATS for class(es) [org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics]

-- Constraints for table PART_PRIVS for class(es) [org.apache.hadoop.hive.metastore.model.MPartitionPrivilege]

-- Constraints for table SKEWED_STRING_LIST for class(es) [org.apache.hadoop.hive.metastore.model.MStringList]

-- Constraints for table ROLES for class(es) [org.apache.hadoop.hive.metastore.model.MRole]

-- Constraints for table PARTITIONS for class(es) [org.apache.hadoop.hive.metastore.model.MPartition]

-- Constraints for table CDS for class(es) [org.apache.hadoop.hive.metastore.model.MColumnDescriptor]

-- Constraints for table VERSION for class(es) [org.apache.hadoop.hive.metastore.model.MVersionTable]

-- Constraints for table GLOBAL_PRIVS for class(es) [org.apache.hadoop.hive.metastore.model.MGlobalPrivilege]

-- Constraints for table PART_COL_PRIVS for class(es) [org.apache.hadoop.hive.metastore.model.MPartitionColumnPrivilege]

-- Constraints for table DB_PRIVS for class(es) [org.apache.hadoop.hive.metastore.model.MDBPrivilege]

-- Constraints for table TAB_COL_STATS for class(es) [org.apache.hadoop.hive.metastore.model.MTableColumnStatistics]

-- Constraints for table TYPES for class(es) [org.apache.hadoop.hive.metastore.model.MType]

-- Constraints for table TBL_PRIVS for class(es) [org.apache.hadoop.hive.metastore.model.MTablePrivilege]

-- Constraints for table DBS for class(es) [org.apache.hadoop.hive.metastore.model.MDatabase]

-- Constraints for table TBL_COL_PRIVS for class(es) [org.apache.hadoop.hive.metastore.model.MTableColumnPrivilege]

-- Constraints for table DELEGATION_TOKENS for class(es) [org.apache.hadoop.hive.metastore.model.MDelegationToken]

-- Constraints for table SERDES for class(es) [org.apache.hadoop.hive.metastore.model.MSerDeInfo]

-- Constraints for table FUNCS for class(es) [org.apache.hadoop.hive.metastore.model.MFunction]
ALTER TABLE FUNCS ADD CONSTRAINT FUNCS_FK1 FOREIGN KEY (DB_ID) REFERENCES DBS (DB_ID) ;

CREATE UNIQUE INDEX UNIQUEFUNCTION ON FUNCS (FUNC_NAME,DB_ID);

CREATE INDEX FUNCS_N49 ON FUNCS (DB_ID);


-- Constraints for table ROLE_MAP for class(es) [org.apache.hadoop.hive.metastore.model.MRoleMap]

-- Constraints for table TBLS for class(es) [org.apache.hadoop.hive.metastore.model.MTable]

-- Constraints for table SDS for class(es) [org.apache.hadoop.hive.metastore.model.MStorageDescriptor]

-- Constraints for table PARTITION_EVENTS for class(es) [org.apache.hadoop.hive.metastore.model.MPartitionEvent]

-- Constraints for table COLUMNS_V2

-- Constraints for table TABLE_PARAMS

-- Constraints for table PARTITION_KEYS

-- Constraints for table SERDE_PARAMS

-- Constraints for table SD_PARAMS

-- Constraints for table FUNC_RU
ALTER TABLE FUNC_RU ADD CONSTRAINT FUNC_RU_FK1 FOREIGN KEY (FUNC_ID) REFERENCES FUNCS (FUNC_ID) ;

CREATE INDEX FUNC_RU_N49 ON FUNC_RU (FUNC_ID);


-- Constraints for table PARTITION_PARAMS

-- Constraints for table SORT_COLS

-- Constraints for table SKEWED_COL_NAMES

-- Constraints for table TYPE_FIELDS

-- Constraints for table DATABASE_PARAMS

-- Constraints for table INDEX_PARAMS

-- Constraints for table BUCKETING_COLS

-- Constraints for table PARTITION_KEY_VALS

-- Constraints for table SKEWED_STRING_LIST_VALUES

-- Constraints for table SKEWED_COL_VALUE_LOC_MAP

-- Constraints for table SKEWED_VALUES


------------------------------------------------------------------
-- Sequences and SequenceTables
