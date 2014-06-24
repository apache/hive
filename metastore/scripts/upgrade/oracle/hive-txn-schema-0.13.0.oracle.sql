-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the License); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an AS IS BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

--
-- Tables for transaction management
-- 

CREATE TABLE TXNS (
  TXN_ID NUMBER(19) PRIMARY KEY,
  TXN_STATE char(1) NOT NULL,
  TXN_STARTED NUMBER(19) NOT NULL,
  TXN_LAST_HEARTBEAT NUMBER(19) NOT NULL,
  TXN_USER varchar(128) NOT NULL,
  TXN_HOST varchar(128) NOT NULL
);

CREATE TABLE TXN_COMPONENTS (
  TC_TXNID NUMBER(19) REFERENCES TXNS (TXN_ID),
  TC_DATABASE VARCHAR2(128) NOT NULL,
  TC_TABLE VARCHAR2(128),
  TC_PARTITION VARCHAR2(767) NULL
);

CREATE TABLE COMPLETED_TXN_COMPONENTS (
  CTC_TXNID NUMBER(19),
  CTC_DATABASE varchar(128) NOT NULL,
  CTC_TABLE varchar(128),
  CTC_PARTITION varchar(767)
);

CREATE TABLE NEXT_TXN_ID (
  NTXN_NEXT NUMBER(19) NOT NULL
);
INSERT INTO NEXT_TXN_ID VALUES(1);

CREATE TABLE HIVE_LOCKS (
  HL_LOCK_EXT_ID NUMBER(19) NOT NULL,
  HL_LOCK_INT_ID NUMBER(19) NOT NULL,
  HL_TXNID NUMBER(19),
  HL_DB VARCHAR2(128) NOT NULL,
  HL_TABLE VARCHAR2(128),
  HL_PARTITION VARCHAR2(767),
  HL_LOCK_STATE CHAR(1) NOT NULL,
  HL_LOCK_TYPE CHAR(1) NOT NULL,
  HL_LAST_HEARTBEAT NUMBER(19) NOT NULL,
  HL_ACQUIRED_AT NUMBER(19),
  HL_USER varchar(128) NOT NULL,
  HL_HOST varchar(128) NOT NULL,
  PRIMARY KEY(HL_LOCK_EXT_ID, HL_LOCK_INT_ID)
); 

CREATE INDEX HL_TXNID_INDEX ON HIVE_LOCKS (HL_TXNID);

CREATE TABLE NEXT_LOCK_ID (
  NL_NEXT NUMBER(19) NOT NULL
);
INSERT INTO NEXT_LOCK_ID VALUES(1);

CREATE TABLE COMPACTION_QUEUE (
  CQ_ID NUMBER(19) PRIMARY KEY,
  CQ_DATABASE varchar(128) NOT NULL,
  CQ_TABLE varchar(128) NOT NULL,
  CQ_PARTITION varchar(767),
  CQ_STATE char(1) NOT NULL,
  CQ_TYPE char(1) NOT NULL,
  CQ_WORKER_ID varchar(128),
  CQ_START NUMBER(19),
  CQ_RUN_AS varchar(128)
);

CREATE TABLE NEXT_COMPACTION_QUEUE_ID (
  NCQ_NEXT NUMBER(19) NOT NULL
);
INSERT INTO NEXT_COMPACTION_QUEUE_ID VALUES(1);


