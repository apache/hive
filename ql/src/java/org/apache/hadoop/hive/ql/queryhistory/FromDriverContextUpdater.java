/**
 * Licensed to the Apache Software Foundation (ASF) under one§
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
package org.apache.hadoop.hive.ql.queryhistory;

import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryInfo;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.tez.TezRuntimeContext;
import org.apache.hadoop.hive.ql.queryhistory.schema.QueryHistoryRecord;
import org.apache.tez.common.counters.DAGCounter;
import org.apache.tez.common.counters.TaskCounter;

public class FromDriverContextUpdater {

  private static final FromDriverContextUpdater INSTANCE = new FromDriverContextUpdater();

  public static FromDriverContextUpdater getInstance() {
    return INSTANCE;
  }

  public void consume(DriverContext driverContext, QueryHistoryRecord record) {
    updateFromQueryState(driverContext.getQueryState(), record);
    updateFromQueryInfo(driverContext.getQueryInfo(), record);
    updateFromRuntimeContext(driverContext.getRuntimeContext(), record);
    updateFromQueryProperties(driverContext.getQueryProperties(), record);

    record.setPlan(driverContext.getExplainPlan());
    record.setQueryType(driverContext.getQueryType());
    record.setDdlType(driverContext.getDdlType());
    record.setFailureReason(driverContext.getQueryErrorMessage());
    record.setNumRowsFetched(driverContext.getFetchTask() == null ? 0 : driverContext.getFetchTask().getTotalRows());
  }

  private void updateFromQueryState(QueryState queryState, QueryHistoryRecord record) {
    record.setQueryId(queryState.getQueryId());
    record.setQuerySql(queryState.getQueryString());
  }

  private void updateFromQueryInfo(QueryInfo queryInfo, QueryHistoryRecord record) {
    if (queryInfo == null) {
      return;
    }
    // this state is the same as "state" displayed in the JSON returned by QueriesRESTfulAPIServlet
    record.setOperationId(queryInfo.getOperationId());
    record.setExecutionEngine(queryInfo.getExecutionEngine());
    record.setQueryState(queryInfo.getState());
    record.setQueryStartTime(queryInfo.getBeginTimeUTC());
    if (queryInfo.getEndTimeUTC() != null) {
      record.setQueryEndTime(queryInfo.getEndTimeUTC());
    }
    record.setTotalTime(queryInfo.getElapsedTime());
  }

  private void updateFromRuntimeContext(TezRuntimeContext runtimeContext, QueryHistoryRecord record) {
    // null in case of HS2-only queries (no Tez involved)
    // dagId null-check is for safety's sake when TezTask was initialized (so a plan was generated) but no DAG ran
    if (runtimeContext == null || runtimeContext.getDagId() == null) {
      setInvalidValuesForRuntimeFields(record);
      return;
    }

    record.setExecutionMode(runtimeContext.getExecutionMode());
    record.setDagId(runtimeContext.getDagId());
    record.setTezApplicationId(runtimeContext.getApplicationId());
    record.setTezSessionId(runtimeContext.getSessionId());
    record.setTezAmAddress(runtimeContext.getAmAddress());
    record.setExecSummary(runtimeContext.getMonitor().getSummary());
    record.setTotalNumberOfTasks((int) runtimeContext.getCounter(DAGCounter.class.getName(),
        DAGCounter.TOTAL_LAUNCHED_TASKS.name()));
    record.setNumberOfSucceededTasks((int) runtimeContext.getCounter(DAGCounter.class.getName(),
        DAGCounter.NUM_SUCCEEDED_TASKS.name()));
    record.setNumberOfKilledTasks((int) runtimeContext.getCounter(DAGCounter.class.getName(),
        DAGCounter.NUM_KILLED_TASKS.name()));
    record.setNumberOfFailedTasks((int) runtimeContext.getCounter(DAGCounter.class.getName(),
        DAGCounter.NUM_FAILED_TASKS.name()));
    record.setTaskDurationMillis(runtimeContext.getCounter(DAGCounter.class.getName(),
        DAGCounter.WALL_CLOCK_MILLIS.name()));
    record.setNodeUsedCount((int) runtimeContext.getCounter(DAGCounter.class.getName(),
        DAGCounter.NODE_USED_COUNT.name()));
    record.setNodeTotalCount((int) runtimeContext.getCounter(DAGCounter.class.getName(),
        DAGCounter.NODE_TOTAL_COUNT.name()));
    record.setReduceInputGroups(runtimeContext.getCounter(TaskCounter.class.getName(),
        TaskCounter.REDUCE_INPUT_GROUPS.name()));
    record.setReduceInputRecords(runtimeContext.getCounter(TaskCounter.class.getName(),
        TaskCounter.REDUCE_INPUT_RECORDS.name()));
    record.setSpilledRecords(runtimeContext.getCounter(TaskCounter.class.getName(),
        TaskCounter.SPILLED_RECORDS.name()));
    record.setNumShuffledInputs(runtimeContext.getCounter(TaskCounter.class.getName(),
        TaskCounter.NUM_SHUFFLED_INPUTS.name()));
    record.setNumFailedShuffleInputs(runtimeContext.getCounter(TaskCounter.class.getName(),
        TaskCounter.NUM_FAILED_SHUFFLE_INPUTS.name()));
    record.setInputRecordsProcessed(runtimeContext.getCounter(TaskCounter.class.getName(),
        TaskCounter.INPUT_RECORDS_PROCESSED.name()));
    record.setInputSplitLengthBytes(runtimeContext.getCounter(TaskCounter.class.getName(),
        TaskCounter.INPUT_SPLIT_LENGTH_BYTES.name()));
    record.setOutputRecords(runtimeContext.getCounter(TaskCounter.class.getName(),
        TaskCounter.OUTPUT_RECORDS.name()));
    record.setOutputBytesPhysical(runtimeContext.getCounter(TaskCounter.class.getName(),
        TaskCounter.OUTPUT_BYTES_PHYSICAL.name()));
    record.setShuffleChunkCount(runtimeContext.getCounter(TaskCounter.class.getName(),
        TaskCounter.SHUFFLE_CHUNK_COUNT.name()));
    record.setShuffleBytes(runtimeContext.getCounter(TaskCounter.class.getName(),
        TaskCounter.SHUFFLE_BYTES.name()));
    record.setShuffleBytesDiskDirect(runtimeContext.getCounter(TaskCounter.class.getName(),
        TaskCounter.SHUFFLE_BYTES_DISK_DIRECT.name()));
    record.setShufflePhaseTime(runtimeContext.getCounter(TaskCounter.class.getName(),
        TaskCounter.SHUFFLE_PHASE_TIME.name()));
    record.setMergePhaseTime(runtimeContext.getCounter(TaskCounter.class.getName(),
        TaskCounter.MERGE_PHASE_TIME.name()));
  }

  private void updateFromQueryProperties(QueryProperties queryProperties, QueryHistoryRecord record) {
    if (queryProperties == null) {
      return;
    }
    record.setTablesQueried(queryProperties.getTablesQueried());
  }

  private void setInvalidValuesForRuntimeFields(QueryHistoryRecord record) {
    record.setTotalNumberOfTasks(-1);
    record.setNumberOfSucceededTasks(-1);
    record.setNumberOfKilledTasks(-1);
    record.setNumberOfFailedTasks(-1);
    record.setTaskDurationMillis(-1L);
    record.setNodeUsedCount(-1);
    record.setNodeTotalCount(-1);
    record.setReduceInputGroups(-1);
    record.setReduceInputRecords(-1);
    record.setSpilledRecords(-1);
    record.setNumShuffledInputs(-1);
    record.setNumFailedShuffleInputs(-1);
    record.setInputRecordsProcessed(-1L);
    record.setInputSplitLengthBytes(-1L);
    record.setOutputRecords(-1L);
    record.setOutputBytesPhysical(-1L);
    record.setShuffleChunkCount(-1);
    record.setShuffleBytes(-1L);
    record.setShuffleBytesDiskDirect(-1L);
    record.setShufflePhaseTime(-1);
    record.setMergePhaseTime(-1);
  }
}
