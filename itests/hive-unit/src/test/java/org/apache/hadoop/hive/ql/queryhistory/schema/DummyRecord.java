/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 *     http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.hadoop.hive.ql.queryhistory.schema;

import com.google.common.collect.Sets;
import org.apache.hadoop.hive.ql.QueryProperties.QueryType;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class DummyRecord extends Record {
  public static final String QUERY_ID = "hive_20240429111756_d39b59fb-31e2-4e89-853e-fac2844530e9";
  public static final String SESSION_ID = "9df26da2-c7f0-4571-838b-f39179a8dcb7";
  public static final String CLUSTER_ID = "hive_instance_123";
  public static final String OPERATION_ID = "hive_operation_id_123";
  public static final String EXECUTION_ENGINE = "tez";
  public static final String EXECUTION_MODE = "llap";
  public static final String DAG_ID = "dag_1714726411929_0001_2";
  public static final String TEZ_APPLICATION_ID = "application_1714726411929_0001";
  public static final String TEZ_SESSION_ID = "8e4f3add-1d11-4738-9789-71c81becd9ce";
  public static final String TEZ_AM_ADDRESS = "localhost:12345";
  public static final String QUERY_STATE = "INITIALIZED";
  public static final String SERVER_HOST = "hs2_host.domain";
  public static final int SERVER_PORT = 100000;
  public static final String SESSION_TYPE = "HIVESERVER2";
  public static final String CLUSTER_USER = "hive";
  public static final String END_USER = "user";
  public static final String SQL = "SELECT 1";
  public static final int HIVERSERVER2_PROTOCOL_VERSION = 1;
  public static final String CURRENT_DATABASE = "default";
  public static final QueryType QUERY_TYPE = QueryType.DQL;
  public static final String OPERATION = "ALTERDATABASE";
  public static final String CLIENT_ADDRESS = "client_host.domain";

  // example durations with distiguishable values (even in case of addition), like: 5100 means planning + prepare plan
  public static final long PLANNING_DURATION = 5000L;
  public static final long PREPARE_PLAN_DURATION = 100L;
  public static final long GET_SESSION_DURATION = 40L;
  public static final long EXECUTION_DURATION = 30000L;

  public static final long TOTAL_TIME = PLANNING_DURATION + PREPARE_PLAN_DURATION + GET_SESSION_DURATION + EXECUTION_DURATION;
  public static final String FAILURE_REASON = "LACK OF LUCK";
  public static final int NUM_ROWS_FETCHED = 1500;
  public static final String PLAN = "This plan is simply awesome";
  public static final String EXEC_SUMMARY = "It was super fast!";
  public static final Set<String> USED_TABLES = Sets.newHashSet(
      "default.queried_table1", "default.queried_table2");
  public static final Map<String, String> CONFIGURATION_OPTIONS_CHANGED = Collections.singletonMap("hive.opt", "hello");
  public static final int TOTAL_LAUNCHED_TASKS = 2100;
  public static final int NUM_SUCCEEDED_TASKS = 2000;
  public static final int NUM_KILLED_TASKS = 100;
  public static final int NUM_FAILED_TASKS = 1;
  public static final long TASK_DURATION_MILLIS = 30000L;
  public static final int NODE_USED_COUNT = 2;
  public static final int NODE_TOTAL_COUNT = 3;
  public static final long REDUCE_INPUT_GROUPS = 1382L;
  public static final long REDUCE_INPUT_RECORDS = 1400000L;
  public static final long SPILLED_RECORDS = 8000L;
  public static final long NUM_SHUFFLED_INPUTS = 400L;
  public static final long NUM_FAILED_SHUFFLE_INPUTS = 2L;
  public static final long INPUT_RECORDS_PROCESSED = 10000000000L;
  public static final long INPUT_SPLIT_LENGTH_BYTES = 30000000000000L;
  public static final long OUTPUT_RECORDS = 40000000L;
  public static final long OUTPUT_BYTES_PHYSICAL = 4500000000L;
  public static final long SHUFFLE_CHUNK_COUNT = 2200L;
  public static final long SHUFFLE_BYTES = 60000000L;
  public static final long SHUFFLE_BYTES_DISK_DIRECT = 13994L;
  public static final long SHUFFLE_PHASE_TIME = 30000L;
  public static final long MERGE_PHASE_TIME = 4000L;

  public DummyRecord() {
    this(System.currentTimeMillis());
  }

  public DummyRecord(long queryStartMillis) {
    setQueryId(QUERY_ID);
    setSessionId(SESSION_ID);
    setClusterId(CLUSTER_ID);
    setOperationId(OPERATION_ID);
    setExecutionEngine(EXECUTION_ENGINE);
    setExecutionMode(EXECUTION_MODE);
    setDagId(DAG_ID);
    setTezApplicationId(TEZ_APPLICATION_ID);
    setTezSessionId(TEZ_SESSION_ID);
    setSessionType(SESSION_TYPE);
    setClusterUser(CLUSTER_USER);
    setEndUser(END_USER);
    setQuerySql(SQL);
    setClientProtocol(HIVERSERVER2_PROTOCOL_VERSION);
    setCurrentDatabase(CURRENT_DATABASE);
    setTezAmAddress(TEZ_AM_ADDRESS);
    setQueryState(QUERY_STATE);
    setQueryType(QUERY_TYPE);
    setOperation(OPERATION);
    setServerAddress(SERVER_HOST);
    setServerPort(SERVER_PORT);
    setClientAddress(CLIENT_ADDRESS);
    setQueryStartTime(queryStartMillis);
    setQueryEndTime(
        queryStartMillis + PLANNING_DURATION + PREPARE_PLAN_DURATION + GET_SESSION_DURATION + EXECUTION_DURATION);
    setTotalTime(TOTAL_TIME);
    setPlanningDuration(PLANNING_DURATION);
    setPlanningStartTime(queryStartMillis);
    setPreparePlanDuration(PREPARE_PLAN_DURATION);
    setPreparePlanStartTime(queryStartMillis + PLANNING_DURATION);
    setGetSessionDuration(GET_SESSION_DURATION);
    setGetSessionStartTime(queryStartMillis + PLANNING_DURATION + PREPARE_PLAN_DURATION);
    setExecutionDuration(EXECUTION_DURATION);
    setExecutionStartTime(queryStartMillis + PLANNING_DURATION + PREPARE_PLAN_DURATION + GET_SESSION_DURATION);
    setFailureReason(FAILURE_REASON);
    setNumRowsFetched(NUM_ROWS_FETCHED);
    setPlan(PLAN);
    setExecSummary(EXEC_SUMMARY);
    setUsedTables(USED_TABLES);
    setConfigurationOptionsChanged(CONFIGURATION_OPTIONS_CHANGED);
    setTotalNumberOfTasks(TOTAL_LAUNCHED_TASKS);
    setNumberOfSucceededTasks(NUM_SUCCEEDED_TASKS);
    setNumberOfKilledTasks(NUM_KILLED_TASKS);
    setNumberOfFailedTasks(NUM_FAILED_TASKS);
    setTaskDurationMillis(TASK_DURATION_MILLIS);
    setNodeUsedCount(NODE_USED_COUNT);
    setNodeTotalCount(NODE_TOTAL_COUNT);
    setReduceInputGroups(REDUCE_INPUT_GROUPS);
    setReduceInputRecords(REDUCE_INPUT_RECORDS);
    setSpilledRecords(SPILLED_RECORDS);
    setNumShuffledInputs(NUM_SHUFFLED_INPUTS);
    setNumFailedShuffleInputs(NUM_FAILED_SHUFFLE_INPUTS);
    setInputRecordsProcessed(INPUT_RECORDS_PROCESSED);
    setInputSplitLengthBytes(INPUT_SPLIT_LENGTH_BYTES);
    setOutputRecords(OUTPUT_RECORDS);
    setOutputBytesPhysical(OUTPUT_BYTES_PHYSICAL);
    setShuffleChunkCount(SHUFFLE_CHUNK_COUNT);
    setShuffleBytes(SHUFFLE_BYTES);
    setShuffleBytesDiskDirect(SHUFFLE_BYTES_DISK_DIRECT);
    setShufflePhaseTime(SHUFFLE_PHASE_TIME);
    setMergePhaseTime(MERGE_PHASE_TIME);
  }
}
