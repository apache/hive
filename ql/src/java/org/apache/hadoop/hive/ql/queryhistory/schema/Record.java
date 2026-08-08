/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.queryhistory.schema;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.queryhistory.schema.Schema.Field;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.io.Writable;
import org.apache.hive.common.util.HiveVersionInfo;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Record {
  private long estimatedSizeInMemoryBytes = 0;

  public Record(){
    set(Field.QUERY_HISTORY_SCHEMA_VERSION, Schema.CURRENT_VERSION);
    set(Field.HIVE_VERSION, HiveVersionInfo.getVersion());
  }

  protected final Map<String, Object> record = new HashMap<>();

  public void setQueryId(String queryId) {
    set(Schema.Field.QUERY_ID, queryId);
  }

  public void setSessionId(String sessionId) {
    set(Schema.Field.SESSION_ID, sessionId);
  }

  public void setOperationId(String operationId) {
    set(Schema.Field.OPERATION_ID, operationId);
  }

  public void setExecutionEngine(String executionEngine) {
    set(Schema.Field.EXECUTION_ENGINE, executionEngine);
  }

  public void setExecutionMode(String executionMode) {
    set(Schema.Field.EXECUTION_MODE, executionMode);
  }

  public void setDagId(String dagId) {
    set(Schema.Field.TEZ_DAG_ID, dagId);
  }

  public void setClusterId(String clusterId) {
    set(Field.CLUSTER_ID, clusterId);
  }

  public void setTezApplicationId(String applicationId) {
    set(Schema.Field.TEZ_APP_ID, applicationId);
  }

  public void setTezSessionId(String sessionId) {
    set(Schema.Field.TEZ_SESSION_ID, sessionId);
  }

  public void setSessionType(String sessionType) {
    set(Schema.Field.SESSION_TYPE, sessionType);
  }

  public void setClientProtocol(int clientProtocol) {
    set(Schema.Field.HIVERSERVER2_PROTOCOL_VERSION, clientProtocol);
  }

  public void setClusterUser(String username) {
    set(Schema.Field.CLUSTER_USER, username);
  }

  public void setEndUser(String userNameConnection) {
    set(Schema.Field.END_USER, userNameConnection);
  }

  public void setQuerySql(String querySql) {
    set(Schema.Field.SQL, querySql);
  }

  public void setCurrentDatabase(String currentDatabase) {
    set(Schema.Field.DB_NAME, currentDatabase);
  }

  public void setTezAmAddress(String amAddress) {
    set(Schema.Field.TEZ_COORDINATOR, amAddress);
  }

  public void setQueryState(String state) {
    set(Schema.Field.QUERY_STATE, state);
  }

  public void setQueryType(QueryProperties.QueryType type) {
    set(Schema.Field.QUERY_TYPE, type == null ? QueryProperties.QueryType.OTHER.getName() :
        type.getName());
  }

  public void setOperation(String operation) {
    set(Field.OPERATION, operation);
  }

  public void setServerAddress(String serverAddress) {
    set(Schema.Field.SERVER_ADDRESS, serverAddress);
  }

  public void setServerPort(int serverPort) {
    set(Schema.Field.SERVER_PORT, serverPort);
  }

  public void setClientAddress(String userIpAddress) {
    set(Schema.Field.CLIENT_ADDRESS, userIpAddress);
  }

  public void setQueryStartTime(long beginTimeEpochMillis) {
    set(Field.START_TIME_UTC, Timestamp.ofEpochMilli(beginTimeEpochMillis));
    set(Field.START_TIME, Timestamp.ofEpochMilli(beginTimeEpochMillis, ZoneId.systemDefault()));
  }

  public void setQueryEndTime(long endTimeEpochMillis) {
    set(Field.END_TIME_UTC, Timestamp.ofEpochMilli(endTimeEpochMillis));
    set(Field.END_TIME, Timestamp.ofEpochMilli(endTimeEpochMillis, ZoneId.systemDefault()));
  }

  public void setTotalTime(long elapsedTime) {
    set(Field.TOTAL_TIME_MS, elapsedTime);
  }

  public void setPlanningDuration(long duration) {
    set(Field.PLANNING_DURATION, duration);
  }

  public void setPlanningStartTime(long startTime) {
    set(Field.PLANNING_START_TIME, timeToNullDefault(startTime));
  }

  public void setPreparePlanDuration(long duration) {
    set(Field.PREPARE_PLAN_DURATION, duration);
  }

  public void setPreparePlanStartTime(long startTime) {
    set(Field.PREPARE_PLAN_START_TIME, timeToNullDefault(startTime));
  }

  public void setGetSessionDuration(long duration) {
    set(Field.GET_SESSION_DURATION, duration);
  }

  public void setGetSessionStartTime(long startTime) {
    set(Field.GET_SESSION_START_TIME, timeToNullDefault(startTime));
  }

  public void setExecutionDuration(long duration) {
    set(Field.EXECUTION_DURATION, duration);
  }

  public void setExecutionStartTime(long startTime) {
    set(Field.EXECUTION_START_TIME, timeToNullDefault(startTime));
  }

  public void setFailureReason(String errorMessage) {
    set(Field.FAILURE_REASON, errorMessage);
  }

  public void setNumRowsFetched(int totalRows) {
    set(Field.NUM_ROWS_FETCHED, totalRows);
  }

  public void setPlan(String explainPlan) {
    set(Field.PLAN, explainPlan);
  }


  public void setUsedTables(Set<String> usedTables) {
    set(Field.USED_TABLES, String.join(",", usedTables));
  }

  public void setExecSummary(String summary) {
    set(Field.EXEC_SUMMARY, summary);
  }

  public void setConfigurationOptionsChanged(Map<String, String> overriddenConfigurations) {
    set(Field.CONFIGURATION_OPTIONS_CHANGED, Joiner.on(";").withKeyValueSeparator("=").join(overriddenConfigurations));
  }

  public void setTotalNumberOfTasks(int numTasks) {
    set(Field.TOTAL_LAUNCHED_TASKS, numTasks);
  }

  public void setNumberOfSucceededTasks(int numSucceededTasks) {
    set(Field.NUM_SUCCEEDED_TASKS, numSucceededTasks);
  }

  public void setNumberOfKilledTasks(int numKilledTasks) {
    set(Field.NUM_KILLED_TASKS, numKilledTasks);
  }

  public void setNumberOfFailedTasks(int numFailedTasks) {
    set(Field.NUM_FAILED_TASKS, numFailedTasks);
  }

  public void setNodeUsedCount(int nodeUsedCount) {
    set(Field.NODE_USED_COUNT, nodeUsedCount);
  }

  public void setNodeTotalCount(int nodeTotalCount) {
    set(Field.NODE_TOTAL_COUNT, nodeTotalCount);
  }

  public void setReduceInputGroups(long reduceInputGroups) {
    set(Field.REDUCE_INPUT_GROUPS, reduceInputGroups);
  }

  public void setReduceInputRecords(long reduceInputRecords) {
    set(Field.REDUCE_INPUT_RECORDS, reduceInputRecords);
  }

  public void setSpilledRecords(long spilledRecords) {
    set(Field.SPILLED_RECORDS, spilledRecords);
  }

  public void setNumShuffledInputs(long numShuffledInputs) {
    set(Field.NUM_SHUFFLED_INPUTS, numShuffledInputs);
  }

  public void setNumFailedShuffleInputs(long numFailedShuffleInputs) {
    set(Field.NUM_FAILED_SHUFFLE_INPUTS, numFailedShuffleInputs);
  }

  public void setTaskDurationMillis(long taskDurationMillis) {
    set(Field.TASK_DURATION_MILLIS, taskDurationMillis);
  }

  public void setInputRecordsProcessed(long inputRecordsProcessed) {
    set(Field.INPUT_RECORDS_PROCESSED, inputRecordsProcessed);
  }

  public void setInputSplitLengthBytes(long inputSplitLengthBytes) {
    set(Field.INPUT_SPLIT_LENGTH_BYTES, inputSplitLengthBytes);
  }

  public void setOutputRecords(long outputRecords) {
    set(Field.OUTPUT_RECORDS, outputRecords);
  }

  public void setOutputBytesPhysical(long outputBytesPhysical) {
    set(Field.OUTPUT_BYTES_PHYSICAL, outputBytesPhysical);
  }

  public void setShuffleChunkCount(long shuffleChunkCount) {
    set(Field.SHUFFLE_CHUNK_COUNT, shuffleChunkCount);
  }

  public void setShuffleBytes(long shuffleBytes) {
    set(Field.SHUFFLE_BYTES, shuffleBytes);
  }

  public void setShuffleBytesDiskDirect(long shuffleBytesDiskDirect) {
    set(Field.SHUFFLE_BYTES_DISK_DIRECT, shuffleBytesDiskDirect);
  }

  public void setShufflePhaseTime(long shufflePhaseTime) {
    set(Field.SHUFFLE_PHASE_TIME, shufflePhaseTime);
  }

  public void setMergePhaseTime(long phaseTime) {
    set(Field.MERGE_PHASE_TIME, phaseTime);
  }

  public Object get(Field field) {
    return record.get(field.name);
  }

  private void set(Field field, Object value) {
    record.put(field.name, value);
  }

  public String toString() {
    return String.format("QueryHistoryRecord [queryId: %s, sessionId: %s]", get(Schema.Field.QUERY_ID),
        get(Schema.Field.SESSION_ID));
  }

  public Writable serialize(Serializer serializer) throws Exception {
    return serializer.serialize(toSerializable(), Schema.INSPECTOR);
  }

  Object[] toSerializable() {
    Object[] record = new Object[Schema.Field.values().length];
    for (Field field : Schema.Field.values()) {
      record[field.ordinal()] = getFieldValue(field);
    }
    return record;
  }

  private Object getFieldValue(Field field) {
    return record.get(field.name);
  }

  private Timestamp timeToNullDefault(long time) {
    return time == 0 ? null : Timestamp.ofEpochMilli(time, ZoneId.systemDefault());
  }

  /**
   * Returns an estimated size of this record in bytes. This is used by the QueryHistoryService to sum the sizes of
   * records to decide if it needs to persist them (to prevent holding too many history records in memory).
   * This is a rough estimation in 2 steps:
   * 1. Given a base size of the presumably "short" fields (String lengths are considered to be a constant)
   * 2. Add the size of the "long" fields
   * @return estimatedSizeInMemoryBytes
   */
  public long getEstimatedSizeInMemoryBytes() {
    if (this.estimatedSizeInMemoryBytes > 0) {
      return this.estimatedSizeInMemoryBytes;
    }
    long calculatedSize = Schema.BASE_RECORD_SIZE_IN_MEMORY_BYTES;

    // consider the "long" fields which mainly contribute to the size of the record
    calculatedSize += getFieldSize(Field.FAILURE_REASON);
    calculatedSize += getFieldSize(Field.CONFIGURATION_OPTIONS_CHANGED);
    calculatedSize += getFieldSize(Field.EXEC_SUMMARY);
    calculatedSize += getFieldSize(Field.PLAN);
    calculatedSize += getFieldSize(Field.SQL);

    this.estimatedSizeInMemoryBytes = calculatedSize;
    return estimatedSizeInMemoryBytes;
  }

  private int getFieldSize(Field field) {
    return Strings.nullToEmpty((String) get(field)).length() * Character.BYTES;
  }
}
