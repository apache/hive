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
package org.apache.hadoop.hive.ql.queryhistory;

import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.ServiceContext;
import org.apache.hadoop.hive.ql.QueryInfo;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.exec.tez.TezRuntimeContext;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.exec.tez.monitoring.TezJobMonitor;
import org.apache.hadoop.hive.ql.queryhistory.schema.DummyQueryHistoryRecord;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.queryhistory.repository.IcebergRepository;
import org.apache.hadoop.hive.ql.queryhistory.repository.QueryHistoryRepository;
import org.apache.hadoop.hive.ql.queryhistory.schema.QueryHistoryRecord;
import org.apache.hadoop.hive.ql.queryhistory.schema.QueryHistorySchema;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.counters.DAGCounter;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounters;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Queue;

public class TestQueryHistoryService {
  private static final Logger LOG = LoggerFactory.getLogger(TestQueryHistoryService.class);
  private static final ServiceContext serviceContext = new ServiceContext(() -> DummyQueryHistoryRecord.SERVER_HOST,
      () ->  DummyQueryHistoryRecord.SERVER_PORT);

  @Test
  public void testDefaultRepositoryIsIceberg() {
    HiveConf conf = getHiveConf();
    QueryHistoryService service = new QueryHistoryService(conf, serviceContext);
    QueryHistoryRepository repository = service.getRepository();
    Assert.assertTrue("Default repository should be iceberg, instead found: " + repository.getClass().getName(),
        repository instanceof IcebergRepository);
  }

  /**
   * This unit test asserts that the record is created as expected from the various hive source objects like:
   * DriverContext, SessionState, QueryInfo, etc.
   */
  @Test
  public void testSimplePersist() throws Exception {
    HiveConf conf = getHiveConfDummyRepository();
    QueryHistoryService service = new QueryHistoryService(conf, serviceContext).start();

    // prepare the source object from which the QueryHistoryService will obtain query information
    QueryInfo queryInfo = spy(new QueryInfo(DummyQueryHistoryRecord.QUERY_STATE, DummyQueryHistoryRecord.END_USER,
        DummyQueryHistoryRecord.EXECUTION_ENGINE, DummyQueryHistoryRecord.SESSION_ID,
        DummyQueryHistoryRecord.OPERATION_ID));
    // elapsed time is calculated from System.currentTimeMillis(), let's mock it here for unit test convenience's sake
    when(queryInfo.getElapsedTime()).thenReturn(DummyQueryHistoryRecord.TOTAL_TIME);
    queryInfo.setEndTime();

    HiveConf.setQueryString(conf, DummyQueryHistoryRecord.SQL);

    QueryState queryState = DriverFactory.getNewQueryState(conf);

    // prepare SessionState
    SessionState ss = SessionState.start(conf);
    prepareSessionState(ss);

    // DriverContext
    DriverContext driverContext = spy(new DriverContext(queryState, queryInfo, null, null));
    prepareDriverContext(driverContext);

    // prepare perfLogger
    PerfLogger perfLogger = SessionState.getPerfLogger();
    preparePerfLogger(queryInfo, perfLogger);

    // QueryPlan
    QueryPlan plan = new QueryPlan();
    plan.setRootTasks(Arrays.asList(generateTezTask()));
    driverContext.setPlan(plan);

    // let the QueryHistoryService consume the data
    service.handleQuery(driverContext);

    // get the prepared record from the service/repository
    DummyQueryHistoryRepository repository = (DummyQueryHistoryRepository)service.getRepository();
    QueryHistoryRecord record = repository.record;

    // check all the values are as expected in the record
    checkValues(ss, queryState, driverContext, record);

    service.stop();
  }

  private static void prepareSessionState(SessionState ss) {
    ss.setIsHiveServerQuery(true);
    ss.setOverriddenConfigurations(DummyQueryHistoryRecord.CONFIGURATION_OPTIONS_CHANGED);
    ss.setUserIpAddress(DummyQueryHistoryRecord.CLIENT_ADDRESS);
    ss.getConf().setInt(SerDeUtils.LIST_SINK_OUTPUT_PROTOCOL, DummyQueryHistoryRecord.HIVERSERVER2_PROTOCOL_VERSION);
  }

  private static void preparePerfLogger(QueryInfo queryInfo, PerfLogger perfLogger) throws NoSuchFieldException,
      IllegalAccessException {
    // this reflection hack is for making us able to mock some values to the thread local perfLogger
    // which is otherwise not accessible
    Field startTimesField = perfLogger.getClass().getDeclaredField("startTimes");
    Field endTimesField = perfLogger.getClass().getDeclaredField("endTimes");
    startTimesField.setAccessible(true);
    endTimesField.setAccessible(true);
    final ConcurrentMap<String, Long> startTimes = (ConcurrentMap)startTimesField.get(perfLogger);
    final ConcurrentMap<String, Long> endTimes = (ConcurrentMap)endTimesField.get(perfLogger);

    // COMPILE (planning)
    startTimes.put(PerfLogger.COMPILE, queryInfo.getBeginTime());
    long compileEndTime = queryInfo.getBeginTime() + DummyQueryHistoryRecord.PLANNING_DURATION;
    endTimes.put(PerfLogger.COMPILE, compileEndTime);

    // GET_SESSION
    long getSessionStartTime = compileEndTime + DummyQueryHistoryRecord.PREPARE_PLAN_DURATION;
    startTimes.put(PerfLogger.TEZ_GET_SESSION, getSessionStartTime);
    long getSessionEndTime = getSessionStartTime + DummyQueryHistoryRecord.GET_SESSION_DURATION;
    endTimes.put(PerfLogger.TEZ_GET_SESSION, getSessionEndTime);

    // let's ignore DAG submission time for the sake of this unit test
    startTimes.put(PerfLogger.TEZ_SUBMIT_DAG, getSessionEndTime);
    endTimes.put(PerfLogger.TEZ_SUBMIT_DAG, getSessionEndTime);

    startTimes.put(PerfLogger.TEZ_RUN_DAG, getSessionEndTime);
    long runTezDagEndTime = getSessionEndTime + DummyQueryHistoryRecord.EXECUTION_DURATION;
    endTimes.put(PerfLogger.TEZ_RUN_DAG, runTezDagEndTime);
  }

  private static void prepareDriverContext(DriverContext driverContext) {
    // mock the queryType and ddlType, because the calculation if those are quite complicated,
    // which should be unit tested separately
    when(driverContext.getQueryType()).thenReturn(DummyQueryHistoryRecord.QUERY_TYPE);
    when(driverContext.getDdlType()).thenReturn(DummyQueryHistoryRecord.DDL_TYPE);
    driverContext.setQueryErrorMessage(DummyQueryHistoryRecord.FAILURE_REASON);
    driverContext.setExplainPlan(DummyQueryHistoryRecord.PLAN);
    FetchTask fetchTask = mock(FetchTask.class);
    when(fetchTask.getTotalRows()).thenReturn(DummyQueryHistoryRecord.NUM_ROWS_FETCHED);
    when(driverContext.getFetchTask()).thenReturn(fetchTask);

    QueryProperties queryProperties = mock(QueryProperties.class);
    when(queryProperties.getTablesQueried()).thenReturn(
        DummyQueryHistoryRecord.TABLES_QUERIED);
    when(driverContext.getQueryProperties()).thenReturn(queryProperties);
  }

  private TezTask generateTezTask() {
    TezTask tezTask = new TezTask();

    TezRuntimeContext runtimeContext = tezTask.getRuntimeContext();

    TezClient tezClient = mock(TezClient.class);
    when(tezClient.getAmHost()).thenReturn(DummyQueryHistoryRecord.TEZ_AM_ADDRESS.split(":")[0]);
    when(tezClient.getAmPort()).thenReturn(Integer.valueOf(DummyQueryHistoryRecord.TEZ_AM_ADDRESS.split(":")[1]));
    runtimeContext.init(tezClient);

    runtimeContext.setDagId(DummyQueryHistoryRecord.DAG_ID);
    runtimeContext.setApplicationId(DummyQueryHistoryRecord.TEZ_APPLICATION_ID);
    runtimeContext.setSessionId(DummyQueryHistoryRecord.TEZ_SESSION_ID);
    runtimeContext.setExecutionMode(DummyQueryHistoryRecord.EXECUTION_MODE);

    TezJobMonitor monitor = mock(TezJobMonitor.class);
    LogHelper console = mock(LogHelper.class);
    when(monitor.getConsole()).thenReturn(console);
    when(console.getSummary()).thenReturn(DummyQueryHistoryRecord.EXEC_SUMMARY);
    runtimeContext.setMonitor(monitor);

    TezCounters counters = new TezCounters();
    runtimeContext.setCounters(counters);
    counters.findCounter(DAGCounter.class.getName(), DAGCounter.TOTAL_LAUNCHED_TASKS.name())
        .setValue(DummyQueryHistoryRecord.TOTAL_LAUNCHED_TASKS);
    counters.findCounter(DAGCounter.class.getName(), DAGCounter.NUM_SUCCEEDED_TASKS.name())
        .setValue(DummyQueryHistoryRecord.NUM_SUCCEEDED_TASKS);
    counters.findCounter(DAGCounter.class.getName(), DAGCounter.NUM_KILLED_TASKS.name())
        .setValue(DummyQueryHistoryRecord.NUM_KILLED_TASKS);
    counters.findCounter(DAGCounter.class.getName(), DAGCounter.NUM_FAILED_TASKS.name())
        .setValue(DummyQueryHistoryRecord.NUM_FAILED_TASKS);
    counters.findCounter(DAGCounter.class.getName(), DAGCounter.WALL_CLOCK_MILLIS.name())
        .setValue(DummyQueryHistoryRecord.TASK_DURATION_MILLIS);
    counters.findCounter(DAGCounter.class.getName(), DAGCounter.NODE_USED_COUNT.name())
        .setValue(DummyQueryHistoryRecord.NODE_USED_COUNT);
    counters.findCounter(DAGCounter.class.getName(), DAGCounter.NODE_TOTAL_COUNT.name())
        .setValue(DummyQueryHistoryRecord.NODE_TOTAL_COUNT);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.REDUCE_INPUT_GROUPS.name())
        .setValue(DummyQueryHistoryRecord.REDUCE_INPUT_GROUPS);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.REDUCE_INPUT_RECORDS.name())
        .setValue(DummyQueryHistoryRecord.REDUCE_INPUT_RECORDS);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.SPILLED_RECORDS.name())
        .setValue(DummyQueryHistoryRecord.SPILLED_RECORDS);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.NUM_SHUFFLED_INPUTS.name())
        .setValue(DummyQueryHistoryRecord.NUM_SHUFFLED_INPUTS);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.NUM_FAILED_SHUFFLE_INPUTS.name())
        .setValue(DummyQueryHistoryRecord.NUM_FAILED_SHUFFLE_INPUTS);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.INPUT_RECORDS_PROCESSED.name())
        .setValue(DummyQueryHistoryRecord.INPUT_RECORDS_PROCESSED);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.INPUT_SPLIT_LENGTH_BYTES.name())
        .setValue(DummyQueryHistoryRecord.INPUT_SPLIT_LENGTH_BYTES);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.OUTPUT_RECORDS.name())
        .setValue(DummyQueryHistoryRecord.OUTPUT_RECORDS);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.OUTPUT_BYTES_PHYSICAL.name())
        .setValue(DummyQueryHistoryRecord.OUTPUT_BYTES_PHYSICAL);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.SHUFFLE_CHUNK_COUNT.name())
        .setValue(DummyQueryHistoryRecord.SHUFFLE_CHUNK_COUNT);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.SHUFFLE_BYTES.name())
        .setValue(DummyQueryHistoryRecord.SHUFFLE_BYTES);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.SHUFFLE_BYTES_DISK_DIRECT.name())
        .setValue(DummyQueryHistoryRecord.SHUFFLE_BYTES_DISK_DIRECT);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.SHUFFLE_PHASE_TIME.name())
        .setValue(DummyQueryHistoryRecord.SHUFFLE_PHASE_TIME);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.MERGE_PHASE_TIME.name())
        .setValue(DummyQueryHistoryRecord.MERGE_PHASE_TIME);

    return tezTask;
  }

  // This method compares the data sources with the values in the QueryHistoryRecord to check if every data made their
  // way there as expected. The expected values are defined as constants from DummyQueryHistoryRecord except where:
  // 1. it was not possible to mock these values (e.g. query_id, session_id), but we can still acquire them for
  // comparison, or
  // 2. their values in the DummyQueryHistoryRecord are not constant and DummyQueryHistoryRecord adds further logic
  // to calculate "realistic" values, making an inner layer which is better to be eliminated (e.g. perfLogger related
  // values)
  private void checkValues(SessionState ss, QueryState queryState, DriverContext driverContext,
      QueryHistoryRecord record) throws Exception {
    // guard counter to make the test fail if a new field is added to the schema which wasn't covered here
    // in case of field addition, it can be decided whether the new field is suitable for testing or not and adapt
    // accordingly
    TezRuntimeContext runtimeContext = driverContext.getRuntimeContext();
    final int ALL_FIELDS =  QueryHistorySchema.Field.values().length;

    Set<QueryHistorySchema.Field> notTestedFields = Sets.newHashSet(
        QueryHistorySchema.Field.QUERY_HISTORY_SCHEMA_VERSION,
        QueryHistorySchema.Field.HIVE_VERSION);
    final int FIELDS_TO_BE_VALIDATED = ALL_FIELDS - notTestedFields.size();
    LOG.info("Comparing input driverContext ({}) to the record to be persisted ({})", driverContext, record);

    AtomicInteger fieldsValidated = new AtomicInteger(0);

    compareValue(QueryHistorySchema.Field.QUERY_ID, queryState.getQueryId(), record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.SESSION_ID, ss.getSessionId(), record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.OPERATION_ID, DummyQueryHistoryRecord.OPERATION_ID, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.EXECUTION_ENGINE, DummyQueryHistoryRecord.EXECUTION_ENGINE, record,
        fieldsValidated);
    compareValue(QueryHistorySchema.Field.EXECUTION_MODE, DummyQueryHistoryRecord.EXECUTION_MODE, record,
        fieldsValidated);
    compareValue(QueryHistorySchema.Field.TEZ_DAG_ID, DummyQueryHistoryRecord.DAG_ID, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.TEZ_APP_ID, DummyQueryHistoryRecord.TEZ_APPLICATION_ID, record,
        fieldsValidated);
    compareValue(QueryHistorySchema.Field.TEZ_SESSION_ID, DummyQueryHistoryRecord.TEZ_SESSION_ID, record,
        fieldsValidated);
    compareValue(QueryHistorySchema.Field.CLUSTER_ID, ServiceContext.findClusterId(), record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.SQL, DummyQueryHistoryRecord.SQL, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.SESSION_TYPE, DummyQueryHistoryRecord.SESSION_TYPE, record,
        fieldsValidated);
    compareValue(QueryHistorySchema.Field.HIVERSERVER2_PROTOCOL_VERSION,
        DummyQueryHistoryRecord.HIVERSERVER2_PROTOCOL_VERSION, record, fieldsValidated);

    boolean doAsEnabled = ss.getConf().getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS);
    compareValue(QueryHistorySchema.Field.CLUSTER_USER,
        doAsEnabled ? ss.getUserName() : Utils.getUGI().getShortUserName(), record, fieldsValidated);

    // doAs=false, END_USER is the current user (usually "hive" on a cluster)
    compareValue(QueryHistorySchema.Field.END_USER, ss.getUserName(), record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.DB_NAME, DummyQueryHistoryRecord.CURRENT_DATABASE, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.TEZ_COORDINATOR, DummyQueryHistoryRecord.TEZ_AM_ADDRESS, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.QUERY_STATE, DummyQueryHistoryRecord.QUERY_STATE, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.QUERY_TYPE, DummyQueryHistoryRecord.QUERY_TYPE.getName(), record,
        fieldsValidated);
    compareValue(QueryHistorySchema.Field.DDL_TYPE, DummyQueryHistoryRecord.DDL_TYPE, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.SERVER_ADDRESS, serviceContext.getHost(), record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.SERVER_PORT, serviceContext.getPort(), record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.CLIENT_ADDRESS, DummyQueryHistoryRecord.CLIENT_ADDRESS, record, fieldsValidated);

    compareValue(QueryHistorySchema.Field.START_TIME_UTC,
        Timestamp.ofEpochMilli(driverContext.getQueryInfo().getBeginTime()), record,
        fieldsValidated);
    compareValue(QueryHistorySchema.Field.START_TIME,
        Timestamp.ofEpochMilli(driverContext.getQueryInfo().getBeginTime(), ZoneId.systemDefault()),
        record,
        fieldsValidated);
    compareValue(QueryHistorySchema.Field.END_TIME_UTC,
        Timestamp.ofEpochMilli(driverContext.getQueryInfo().getEndTime()), record,
        fieldsValidated);
    compareValue(QueryHistorySchema.Field.END_TIME,
        Timestamp.ofEpochMilli(driverContext.getQueryInfo().getEndTime(), ZoneId.systemDefault()),
        record,
        fieldsValidated);

    compareValue(QueryHistorySchema.Field.TOTAL_TIME_MS, DummyQueryHistoryRecord.TOTAL_TIME, record, fieldsValidated);

    // perfLogger values
    compareValue(QueryHistorySchema.Field.PLANNING_DURATION,
        SessionState.getPerfLogger().getEndTime(PerfLogger.COMPILE) - SessionState.getPerfLogger()
            .getStartTime(PerfLogger.COMPILE),
        record,
        fieldsValidated);
    compareValue(QueryHistorySchema.Field.PLANNING_START_TIME,
        Timestamp.ofEpochMilli(SessionState.getPerfLogger().getStartTime(PerfLogger.COMPILE), ZoneId.systemDefault()),
        record,
        fieldsValidated);
    compareValue(QueryHistorySchema.Field.PREPARE_PLAN_DURATION,
        SessionState.getPerfLogger().getPreparePlanDuration(),
        record,
        fieldsValidated);
    compareValue(QueryHistorySchema.Field.PREPARE_PLAN_START_TIME,
        Timestamp.ofEpochMilli(SessionState.getPerfLogger().getEndTime(PerfLogger.COMPILE), ZoneId.systemDefault()),
        record,
        fieldsValidated);
    compareValue(QueryHistorySchema.Field.GET_SESSION_DURATION,
        SessionState.getPerfLogger().getEndTime(PerfLogger.TEZ_GET_SESSION) - SessionState.getPerfLogger()
            .getStartTime(PerfLogger.TEZ_GET_SESSION), record,
        fieldsValidated);
    compareValue(QueryHistorySchema.Field.GET_SESSION_START_TIME,
        Timestamp.ofEpochMilli(SessionState.getPerfLogger().getStartTime(PerfLogger.TEZ_GET_SESSION),
            ZoneId.systemDefault()), record,
        fieldsValidated);
    compareValue(QueryHistorySchema.Field.EXECUTION_DURATION,
        SessionState.getPerfLogger().getEndTime(PerfLogger.TEZ_RUN_DAG) - SessionState.getPerfLogger()
            .getStartTime(PerfLogger.TEZ_RUN_DAG), record,
        fieldsValidated);
    compareValue(QueryHistorySchema.Field.EXECUTION_START_TIME,
        Timestamp.ofEpochMilli(SessionState.getPerfLogger().getStartTime(PerfLogger.TEZ_RUN_DAG),
            ZoneId.systemDefault()), record,
        fieldsValidated);

    compareValue(QueryHistorySchema.Field.FAILURE_REASON, DummyQueryHistoryRecord.FAILURE_REASON, record,
        fieldsValidated);
    compareValue(QueryHistorySchema.Field.NUM_ROWS_FETCHED, DummyQueryHistoryRecord.NUM_ROWS_FETCHED, record,
        fieldsValidated);
    compareValue(QueryHistorySchema.Field.PLAN, DummyQueryHistoryRecord.PLAN, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.TABLES_QUERIED, String.join(",",
        DummyQueryHistoryRecord.TABLES_QUERIED), record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.EXEC_SUMMARY, DummyQueryHistoryRecord.EXEC_SUMMARY, record,
        fieldsValidated);
    compareValue(QueryHistorySchema.Field.CONFIGURATION_OPTIONS_CHANGED,
        Joiner.on(";").withKeyValueSeparator("=").join(DummyQueryHistoryRecord.CONFIGURATION_OPTIONS_CHANGED),
        record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.TOTAL_LAUNCHED_TASKS,
        DummyQueryHistoryRecord.TOTAL_LAUNCHED_TASKS, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.NUM_SUCCEEDED_TASKS,
        DummyQueryHistoryRecord.NUM_SUCCEEDED_TASKS, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.NUM_KILLED_TASKS,
        DummyQueryHistoryRecord.NUM_KILLED_TASKS, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.NUM_FAILED_TASKS,
        DummyQueryHistoryRecord.NUM_FAILED_TASKS, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.TASK_DURATION_MILLIS,
        DummyQueryHistoryRecord.TASK_DURATION_MILLIS, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.NODE_USED_COUNT,
        DummyQueryHistoryRecord.NODE_USED_COUNT, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.NODE_TOTAL_COUNT,
        DummyQueryHistoryRecord.NODE_TOTAL_COUNT, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.REDUCE_INPUT_GROUPS,
        DummyQueryHistoryRecord.REDUCE_INPUT_GROUPS, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.REDUCE_INPUT_RECORDS,
        DummyQueryHistoryRecord.REDUCE_INPUT_RECORDS, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.SPILLED_RECORDS,
        DummyQueryHistoryRecord.SPILLED_RECORDS, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.NUM_SHUFFLED_INPUTS,
        DummyQueryHistoryRecord.NUM_SHUFFLED_INPUTS, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.NUM_FAILED_SHUFFLE_INPUTS,
        DummyQueryHistoryRecord.NUM_FAILED_SHUFFLE_INPUTS, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.INPUT_RECORDS_PROCESSED,
        DummyQueryHistoryRecord.INPUT_RECORDS_PROCESSED, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.INPUT_SPLIT_LENGTH_BYTES,
        DummyQueryHistoryRecord.INPUT_SPLIT_LENGTH_BYTES, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.OUTPUT_RECORDS,
        DummyQueryHistoryRecord.OUTPUT_RECORDS, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.OUTPUT_BYTES_PHYSICAL,
        DummyQueryHistoryRecord.OUTPUT_BYTES_PHYSICAL, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.SHUFFLE_CHUNK_COUNT,
        DummyQueryHistoryRecord.SHUFFLE_CHUNK_COUNT, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.SHUFFLE_BYTES,
        DummyQueryHistoryRecord.SHUFFLE_BYTES, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.SHUFFLE_BYTES_DISK_DIRECT,
        DummyQueryHistoryRecord.SHUFFLE_BYTES_DISK_DIRECT, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.SHUFFLE_PHASE_TIME,
        DummyQueryHistoryRecord.SHUFFLE_PHASE_TIME, record, fieldsValidated);
    compareValue(QueryHistorySchema.Field.MERGE_PHASE_TIME,
        DummyQueryHistoryRecord.MERGE_PHASE_TIME, record, fieldsValidated);

    if (fieldsValidated.get() != FIELDS_TO_BE_VALIDATED) {
      Assert.fail(
          "Less (or more?) fields have been validated than expected: all: " + ALL_FIELDS + ", expected: "
              + FIELDS_TO_BE_VALIDATED + ", actual: " + fieldsValidated.get());
    }
  }

  // This method compares a single field in the record to the original value (which is expected to be in the record)
  private void compareValue(QueryHistorySchema.Field field, Object originalValue, QueryHistoryRecord record,
      AtomicInteger fieldsValidated) {
    Object valueInRecord = record.get(field);
    Assert.assertEquals(field + " values are not equal", originalValue, valueInRecord);
    fieldsValidated.incrementAndGet();
  }


  @Test
  public void testBatchSizeBasedPersist() {
    HiveConf conf = getHiveConfDummyRepository();
    QueryHistoryRecord record = new QueryHistoryRecord();
    // make sure we don't persist according to estimated memory size
    conf.setIntVar(HiveConf.ConfVars.HIVE_QUERY_HISTORY_PERSIST_MAX_MEMORY_BYTES, 1000000);
    // persist after the third record
    conf.setIntVar(HiveConf.ConfVars.HIVE_QUERY_HISTORY_PERSIST_MAX_BATCH_SIZE, 3);

    QueryHistoryServiceForTest service = new QueryHistoryServiceForTest(conf, serviceContext);

    Assert.assertFalse("Call to persist is not expected here (no records)",
        service.needToPersist());

    service.addRecordDirectly(record);
    Assert.assertFalse("Call to persist is not expected here (1 record)",
        service.needToPersist());

    service.addRecordDirectly(record);
    Assert.assertFalse("Call to persist is not expected here (2 records)",
        service.needToPersist());

    service.addRecordDirectly(record);
    Assert.assertTrue("Call to persist is expected here (3 records == configured limit)",
        service.needToPersist());
  }

  @Test
  public void testMemoryConsumptionBasedPersist() {
    HiveConf conf = getHiveConfDummyRepository();
    QueryHistoryRecord record = new QueryHistoryRecord();
    long recordSize = record.getEstimatedSizeInMemoryBytes();
    // make sure we don't persist according to batch size
    conf.setIntVar(HiveConf.ConfVars.HIVE_QUERY_HISTORY_PERSIST_MAX_BATCH_SIZE, 1000);
    // persist after the second record (2 * recordSize > recordSize + 1)
    conf.setIntVar(HiveConf.ConfVars.HIVE_QUERY_HISTORY_PERSIST_MAX_MEMORY_BYTES, (int)recordSize + 1);

    QueryHistoryServiceForTest service = new QueryHistoryServiceForTest(conf, serviceContext);

    Assert.assertFalse("Call to persist is not expected here (no records)",
        service.needToPersist());

    // add the record once: nothing is expected to happen
    service.addRecordDirectly(record);
    Assert.assertFalse("Call to persist is not expected here (1 record, didn't reach either of the limits)",
        service.needToPersist());

    // add the record again (same size): needToPersist is supposed to return true
    service.addRecordDirectly(record);
    Assert.assertTrue("Call to persist is expected here (memory size exceeded)",
        service.needToPersist());
  }

  private HiveConf getHiveConf() {
    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.HIVE_CLI_TEZ_INITIALIZE_SESSION, false);

    return conf;
  }

  private HiveConf getHiveConfDummyRepository() {
    HiveConf conf = getHiveConf();

    conf.setVar(HiveConf.ConfVars.HIVE_QUERY_HISTORY_REPOSITORY_CLASS,
        DummyQueryHistoryRepository.class.getName());
    conf.setIntVar(HiveConf.ConfVars.HIVE_QUERY_HISTORY_PERSIST_MAX_BATCH_SIZE, 0);
    return conf;
  }

  static class DummyQueryHistoryRepository implements QueryHistoryRepository {
    QueryHistoryRecord record = null;

    @Override
    public void init(HiveConf conf, QueryHistorySchema schema) {
      LOG.info("DummyQueryHistoryRepository init");
    }

    @Override
    public void persist(Queue<QueryHistoryRecord> records) {
      LOG.info("DummyQueryHistoryRepository to persist {} records", records.size());
      // saving for unit test purposes
      this.record = records.poll();
      records.clear();
    }
  }

  static class QueryHistoryServiceForTest extends QueryHistoryService {
    public QueryHistoryServiceForTest(HiveConf conf, ServiceContext serviceContext) {
      super(conf, serviceContext);
    }

    public void addRecordDirectly(QueryHistoryRecord record) {
      queryHistoryQueue.add(record);
    }
  }
}
