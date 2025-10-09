/**
 * Licensed to the Apache Software Foundation (ASF) under one§
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
 */
package org.apache.hadoop.hive.ql.queryhistory;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryInfo;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ServiceContext;
import org.apache.hadoop.hive.ql.exec.tez.TezRuntimeContext;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.queryhistory.schema.Record;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.tez.common.counters.DAGCounter;
import org.apache.tez.common.counters.TaskCounter;

public class RecordEnricher {
  private final DriverContext driverContext;
  private final ServiceContext serviceContext;
  private final SessionState sessionState;
  private final PerfLogger perfLogger;

  public RecordEnricher(DriverContext driverContext, ServiceContext serviceContext, SessionState sessionState,
      PerfLogger perfLogger) {
    this.driverContext = driverContext;
    this.serviceContext = serviceContext;
    this.sessionState = sessionState;
    this.perfLogger = perfLogger;
  }

  public Record createRecord() {
    Record record = new Record();

    enrichFromDriverContext(record);
    enrichFromSessionState(record);
    enrichFromPerfLogger(record);
    enrichFromServiceContext(record);

    return record;
  }

  private void enrichFromDriverContext(Record record) {
    enrichFromQueryState(driverContext.getQueryState(), record);
    enrichFromQueryInfo(driverContext.getQueryInfo(), record);
    enrichFromRuntimeContext(driverContext.getRuntimeContext(), record);
    enrichFromQueryProperties(driverContext.getQueryProperties(), record);

    record.setPlan(driverContext.getExplainPlan());
    record.setQueryType(driverContext.getQueryType());
    record.setOperation(getClassifiedOperation());
    record.setFailureReason(driverContext.getQueryErrorMessage());
    record.setNumRowsFetched(driverContext.getFetchTask() == null ? 0 : driverContext.getFetchTask().getNumRows());
  }

  /**
   * With query history, a more detailed HiveOperation-based query classification is introduced without breaking the
   * original one. If there is an SqlKind stored in the QueryState, the record will contain it.
   */
  private String getClassifiedOperation() {
    QueryState queryState = driverContext.getQueryState();
    return queryState.getSqlKind() == null ? queryState.getCommandType() : queryState.getSqlKind().toString();
  }

  private void enrichFromSessionState(Record record) {
    record.setSessionId(sessionState.getSessionId());
    record.setEndUser(sessionState.getUserName());
    record.setClientProtocol(sessionState.getConf().getInt(SerDeUtils.LIST_SINK_OUTPUT_PROTOCOL, 0));
    try {
      boolean doAsEnabled = sessionState.getConf().getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS);
      String user = Utils.getUGI().getShortUserName();
      record.setClusterUser(doAsEnabled ? sessionState.getUserName() : user);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    record.setSessionType(
        sessionState.isHiveServerQuery() ? SessionType.HIVESERVER2.name() :
            SessionType.OTHER.name());
    record.setCurrentDatabase(sessionState.getCurrentDatabase());
    record.setClientAddress(sessionState.getUserIpAddress());
    record.setConfigurationOptionsChanged(sessionState.getOverriddenConfigurations());
  }

  private void enrichFromPerfLogger(Record record) {
    record.setPlanningDuration(perfLogger.getEndTime(PerfLogger.COMPILE) - perfLogger.getStartTime(PerfLogger.COMPILE));
    record.setPlanningStartTime(perfLogger.getStartTime(PerfLogger.COMPILE));

    record.setPreparePlanDuration(perfLogger.getPreparePlanDuration());
    record.setPreparePlanStartTime(perfLogger.getEndTime(PerfLogger.COMPILE));

    record.setGetSessionDuration(perfLogger.getDuration(PerfLogger.TEZ_GET_SESSION));
    record.setGetSessionStartTime(perfLogger.getStartTime(PerfLogger.TEZ_GET_SESSION));

    record.setExecutionDuration(perfLogger.getRunDagDuration());
    record.setExecutionStartTime(perfLogger.getStartTime(PerfLogger.TEZ_RUN_DAG));
  }

  private void enrichFromServiceContext(Record record) {
    record.setClusterId(serviceContext.getClusterId());
    record.setServerAddress(serviceContext.getHost());
    record.setServerPort(serviceContext.getPort());
  }

  private void enrichFromQueryState(QueryState queryState, Record record) {
    record.setQueryId(queryState.getQueryId());
    record.setQuerySql(queryState.getQueryString());
  }

  private void enrichFromQueryInfo(QueryInfo queryInfo, Record record) {
    if (queryInfo == null) {
      return;
    }
    // this state is the same as "state" displayed in the JSON returned by QueriesRESTfulAPIServlet
    record.setOperationId(queryInfo.getOperationId());
    record.setExecutionEngine(queryInfo.getExecutionEngine());
    record.setQueryState(queryInfo.getState());
    record.setQueryStartTime(queryInfo.getBeginTime());
    if (queryInfo.getEndTime() != null) {
      record.setQueryEndTime(queryInfo.getEndTime());
    }
    record.setTotalTime(queryInfo.getElapsedTime());
  }

  private void enrichFromRuntimeContext(TezRuntimeContext runtimeContext, Record record) {
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
    record.setExecSummary(runtimeContext.getMonitor().logger().getQuerySummary());
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

  private void enrichFromQueryProperties(QueryProperties queryProperties, Record record) {
    if (queryProperties == null) {
      return;
    }
    record.setUsedTables(queryProperties.getUsedTables());
  }

  private void setInvalidValuesForRuntimeFields(Record record) {
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

  // Enum to represent the type of session, currently only two types are supported
  // based on sessionState.isHiveServerQuery()
  public enum SessionType {
    HIVESERVER2, OTHER
  }
}
