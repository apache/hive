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

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.QueryInfo;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ServiceContext;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.tez.TezRuntimeContext;
import org.apache.hadoop.hive.ql.exec.tez.TezSessionState;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.exec.tez.monitoring.TezJobMonitor;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.queryhistory.repository.IcebergRepository;
import org.apache.hadoop.hive.ql.queryhistory.repository.QueryHistoryRepository;
import org.apache.hadoop.hive.ql.queryhistory.schema.DummyRecord;
import org.apache.hadoop.hive.ql.queryhistory.schema.Record;
import org.apache.hadoop.hive.ql.queryhistory.schema.Schema;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.tez.common.counters.DAGCounter;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounters;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestQueryHistoryService {
  private static final Logger LOG = LoggerFactory.getLogger(TestQueryHistoryService.class);
  private static final ServiceContext serviceContext = new ServiceContext(() -> DummyRecord.SERVER_HOST,
      () ->  DummyRecord.SERVER_PORT);

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
  public void testSimpleFlush() throws Exception {
    HiveConf conf = getHiveConfDummyRepository();
    QueryHistoryService service = new QueryHistoryService(conf, serviceContext);
    service.start();

    // prepare the source object from which the QueryHistoryService will obtain query information
    QueryInfo queryInfo = spy(new QueryInfo(DummyRecord.QUERY_STATE, DummyRecord.END_USER,
        DummyRecord.EXECUTION_ENGINE, DummyRecord.SESSION_ID,
        DummyRecord.OPERATION_ID));
    // elapsed time is calculated from System.currentTimeMillis(), let's mock it here for unit test convenience's sake
    when(queryInfo.getElapsedTime()).thenReturn(DummyRecord.TOTAL_TIME);
    queryInfo.setEndTime();

    HiveConf.setQueryString(conf, DummyRecord.SQL);

    QueryState queryState = DriverFactory.getNewQueryState(conf);
    queryState.setCommandType(HiveOperation.valueOf(DummyRecord.OPERATION));

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
    service.logQuery(driverContext);

    // get the prepared record from the service/repository
    DummyQueryHistoryRepository repository = (DummyQueryHistoryRepository)service.getRepository();
    Record record = repository.record;

    // check all the values are as expected in the record
    checkValues(ss, queryState, driverContext, record);

    service.stop();
  }

  private static void prepareSessionState(SessionState ss) {
    ss.setIsHiveServerQuery(true);
    ss.setOverriddenConfigurations(DummyRecord.CONFIGURATION_OPTIONS_CHANGED);
    ss.setUserIpAddress(DummyRecord.CLIENT_ADDRESS);
    ss.getConf().setInt(SerDeUtils.LIST_SINK_OUTPUT_PROTOCOL, DummyRecord.HIVERSERVER2_PROTOCOL_VERSION);
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
    long compileEndTime = queryInfo.getBeginTime() + DummyRecord.PLANNING_DURATION;
    endTimes.put(PerfLogger.COMPILE, compileEndTime);

    // GET_SESSION
    long getSessionStartTime = compileEndTime + DummyRecord.PREPARE_PLAN_DURATION;
    startTimes.put(PerfLogger.TEZ_GET_SESSION, getSessionStartTime);
    long getSessionEndTime = getSessionStartTime + DummyRecord.GET_SESSION_DURATION;
    endTimes.put(PerfLogger.TEZ_GET_SESSION, getSessionEndTime);

    // let's ignore DAG submission time for the sake of this unit test
    startTimes.put(PerfLogger.TEZ_SUBMIT_DAG, getSessionEndTime);
    endTimes.put(PerfLogger.TEZ_SUBMIT_DAG, getSessionEndTime);

    startTimes.put(PerfLogger.TEZ_RUN_DAG, getSessionEndTime);
    long runTezDagEndTime = getSessionEndTime + DummyRecord.EXECUTION_DURATION;
    endTimes.put(PerfLogger.TEZ_RUN_DAG, runTezDagEndTime);
  }

  private static void prepareDriverContext(DriverContext driverContext) {
    // mock the queryType and operation, because the calculation if those are quite complicated,
    // which should be unit tested separately
    when(driverContext.getQueryType()).thenReturn(DummyRecord.QUERY_TYPE);
    driverContext.setQueryErrorMessage(DummyRecord.FAILURE_REASON);
    driverContext.setExplainPlan(DummyRecord.PLAN);
    FetchTask fetchTask = mock(FetchTask.class);
    when(fetchTask.getNumRows()).thenReturn(DummyRecord.NUM_ROWS_FETCHED);
    when(driverContext.getFetchTask()).thenReturn(fetchTask);

    QueryProperties queryProperties = mock(QueryProperties.class);
    when(queryProperties.getUsedTables()).thenReturn(DummyRecord.USED_TABLES);
    when(driverContext.getQueryProperties()).thenReturn(queryProperties);
  }

  private TezTask generateTezTask() {
    TezTask tezTask = new TezTask();

    TezRuntimeContext runtimeContext = tezTask.getRuntimeContext();

    TezSessionState tezSessionState = mock(TezSessionState.class);
    when(tezSessionState.getAppMasterUri()).thenReturn(DummyRecord.TEZ_AM_ADDRESS);
    runtimeContext.init(tezSessionState);

    runtimeContext.setDagId(DummyRecord.DAG_ID);
    runtimeContext.setApplicationId(DummyRecord.TEZ_APPLICATION_ID);
    runtimeContext.setSessionId(DummyRecord.TEZ_SESSION_ID);
    runtimeContext.setExecutionMode(DummyRecord.EXECUTION_MODE);

    TezJobMonitor monitor = mock(TezJobMonitor.class);
    LogHelper console = mock(LogHelper.class);
    when(monitor.logger()).thenReturn(console);
    when(console.getQuerySummary()).thenReturn(DummyRecord.EXEC_SUMMARY);
    runtimeContext.setMonitor(monitor);

    TezCounters counters = new TezCounters();
    runtimeContext.setCounters(counters);
    counters.findCounter(DAGCounter.class.getName(), DAGCounter.TOTAL_LAUNCHED_TASKS.name())
        .setValue(DummyRecord.TOTAL_LAUNCHED_TASKS);
    counters.findCounter(DAGCounter.class.getName(), DAGCounter.NUM_SUCCEEDED_TASKS.name())
        .setValue(DummyRecord.NUM_SUCCEEDED_TASKS);
    counters.findCounter(DAGCounter.class.getName(), DAGCounter.NUM_KILLED_TASKS.name())
        .setValue(DummyRecord.NUM_KILLED_TASKS);
    counters.findCounter(DAGCounter.class.getName(), DAGCounter.NUM_FAILED_TASKS.name())
        .setValue(DummyRecord.NUM_FAILED_TASKS);
    counters.findCounter(DAGCounter.class.getName(), DAGCounter.WALL_CLOCK_MILLIS.name())
        .setValue(DummyRecord.TASK_DURATION_MILLIS);
    counters.findCounter(DAGCounter.class.getName(), DAGCounter.NODE_USED_COUNT.name())
        .setValue(DummyRecord.NODE_USED_COUNT);
    counters.findCounter(DAGCounter.class.getName(), DAGCounter.NODE_TOTAL_COUNT.name())
        .setValue(DummyRecord.NODE_TOTAL_COUNT);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.REDUCE_INPUT_GROUPS.name())
        .setValue(DummyRecord.REDUCE_INPUT_GROUPS);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.REDUCE_INPUT_RECORDS.name())
        .setValue(DummyRecord.REDUCE_INPUT_RECORDS);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.SPILLED_RECORDS.name())
        .setValue(DummyRecord.SPILLED_RECORDS);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.NUM_SHUFFLED_INPUTS.name())
        .setValue(DummyRecord.NUM_SHUFFLED_INPUTS);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.NUM_FAILED_SHUFFLE_INPUTS.name())
        .setValue(DummyRecord.NUM_FAILED_SHUFFLE_INPUTS);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.INPUT_RECORDS_PROCESSED.name())
        .setValue(DummyRecord.INPUT_RECORDS_PROCESSED);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.INPUT_SPLIT_LENGTH_BYTES.name())
        .setValue(DummyRecord.INPUT_SPLIT_LENGTH_BYTES);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.OUTPUT_RECORDS.name())
        .setValue(DummyRecord.OUTPUT_RECORDS);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.OUTPUT_BYTES_PHYSICAL.name())
        .setValue(DummyRecord.OUTPUT_BYTES_PHYSICAL);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.SHUFFLE_CHUNK_COUNT.name())
        .setValue(DummyRecord.SHUFFLE_CHUNK_COUNT);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.SHUFFLE_BYTES.name())
        .setValue(DummyRecord.SHUFFLE_BYTES);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.SHUFFLE_BYTES_DISK_DIRECT.name())
        .setValue(DummyRecord.SHUFFLE_BYTES_DISK_DIRECT);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.SHUFFLE_PHASE_TIME.name())
        .setValue(DummyRecord.SHUFFLE_PHASE_TIME);
    counters.findCounter(TaskCounter.class.getName(), TaskCounter.MERGE_PHASE_TIME.name())
        .setValue(DummyRecord.MERGE_PHASE_TIME);

    return tezTask;
  }

  // This method compares the data sources with the values in the Record to check if every data made their
  // way there as expected. The expected values are defined as constants from DummyRecord except where:
  // 1. it was not possible to mock these values (e.g. query_id, session_id), but we can still acquire them for
  // comparison, or
  // 2. their values in the DummyRecord are not constant and DummyRecord adds further logic
  // to calculate "realistic" values, making an inner layer which is better to be eliminated (e.g. perfLogger related
  // values)
  private void checkValues(SessionState ss, QueryState queryState, DriverContext driverContext,
      Record record) throws Exception {
    // guard counter to make the test fail if a new field is added to the schema which wasn't covered here
    // in case of field addition, it can be decided whether the new field is suitable for testing or not and adapt
    // accordingly
    TezRuntimeContext runtimeContext = driverContext.getRuntimeContext();
    final int ALL_FIELDS =  Schema.Field.values().length;

    Set<Schema.Field> notTestedFields = Sets.newHashSet(
        Schema.Field.QUERY_HISTORY_SCHEMA_VERSION,
        Schema.Field.HIVE_VERSION);
    final int FIELDS_TO_BE_VALIDATED = ALL_FIELDS - notTestedFields.size();
    LOG.info("Comparing input driverContext ({}) to the record to be flushed ({})", driverContext, record);

    AtomicInteger fieldsValidated = new AtomicInteger(0);

    compareValue(Schema.Field.QUERY_ID, queryState.getQueryId(), record, fieldsValidated);
    compareValue(Schema.Field.SESSION_ID, ss.getSessionId(), record, fieldsValidated);
    compareValue(Schema.Field.OPERATION_ID, DummyRecord.OPERATION_ID, record, fieldsValidated);
    compareValue(Schema.Field.EXECUTION_ENGINE, DummyRecord.EXECUTION_ENGINE, record,
        fieldsValidated);
    compareValue(Schema.Field.EXECUTION_MODE, DummyRecord.EXECUTION_MODE, record,
        fieldsValidated);
    compareValue(Schema.Field.TEZ_DAG_ID, DummyRecord.DAG_ID, record, fieldsValidated);
    compareValue(Schema.Field.TEZ_APP_ID, DummyRecord.TEZ_APPLICATION_ID, record,
        fieldsValidated);
    compareValue(Schema.Field.TEZ_SESSION_ID, DummyRecord.TEZ_SESSION_ID, record,
        fieldsValidated);
    compareValue(Schema.Field.CLUSTER_ID, ServiceContext.findClusterId(), record, fieldsValidated);
    compareValue(Schema.Field.SQL, DummyRecord.SQL, record, fieldsValidated);
    compareValue(Schema.Field.SESSION_TYPE, DummyRecord.SESSION_TYPE, record,
        fieldsValidated);
    compareValue(Schema.Field.HIVERSERVER2_PROTOCOL_VERSION,
        DummyRecord.HIVERSERVER2_PROTOCOL_VERSION, record, fieldsValidated);

    boolean doAsEnabled = ss.getConf().getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS);
    compareValue(Schema.Field.CLUSTER_USER,
        doAsEnabled ? ss.getUserName() : Utils.getUGI().getShortUserName(), record, fieldsValidated);

    // doAs=false, END_USER is the current user (usually "hive" on a cluster)
    compareValue(Schema.Field.END_USER, ss.getUserName(), record, fieldsValidated);
    compareValue(Schema.Field.DB_NAME, DummyRecord.CURRENT_DATABASE, record, fieldsValidated);
    compareValue(Schema.Field.TEZ_COORDINATOR, DummyRecord.TEZ_AM_ADDRESS, record, fieldsValidated);
    compareValue(Schema.Field.QUERY_STATE, DummyRecord.QUERY_STATE, record, fieldsValidated);
    compareValue(Schema.Field.QUERY_TYPE, DummyRecord.QUERY_TYPE.getName(), record,
        fieldsValidated);
    compareValue(Schema.Field.OPERATION, DummyRecord.OPERATION, record, fieldsValidated);
    compareValue(Schema.Field.SERVER_ADDRESS, serviceContext.getHost(), record, fieldsValidated);
    compareValue(Schema.Field.SERVER_PORT, serviceContext.getPort(), record, fieldsValidated);
    compareValue(Schema.Field.CLIENT_ADDRESS, DummyRecord.CLIENT_ADDRESS, record, fieldsValidated);

    compareValue(Schema.Field.START_TIME_UTC,
        Timestamp.ofEpochMilli(driverContext.getQueryInfo().getBeginTime()), record,
        fieldsValidated);
    compareValue(Schema.Field.START_TIME,
        Timestamp.ofEpochMilli(driverContext.getQueryInfo().getBeginTime(), ZoneId.systemDefault()),
        record,
        fieldsValidated);
    compareValue(Schema.Field.END_TIME_UTC,
        Timestamp.ofEpochMilli(driverContext.getQueryInfo().getEndTime()), record,
        fieldsValidated);
    compareValue(Schema.Field.END_TIME,
        Timestamp.ofEpochMilli(driverContext.getQueryInfo().getEndTime(), ZoneId.systemDefault()),
        record,
        fieldsValidated);

    compareValue(Schema.Field.TOTAL_TIME_MS, DummyRecord.TOTAL_TIME, record, fieldsValidated);

    // perfLogger values
    compareValue(Schema.Field.PLANNING_DURATION,
        SessionState.getPerfLogger().getEndTime(PerfLogger.COMPILE) - SessionState.getPerfLogger()
            .getStartTime(PerfLogger.COMPILE),
        record,
        fieldsValidated);
    compareValue(Schema.Field.PLANNING_START_TIME,
        Timestamp.ofEpochMilli(SessionState.getPerfLogger().getStartTime(PerfLogger.COMPILE), ZoneId.systemDefault()),
        record,
        fieldsValidated);
    compareValue(Schema.Field.PREPARE_PLAN_DURATION,
        SessionState.getPerfLogger().getPreparePlanDuration(),
        record,
        fieldsValidated);
    compareValue(Schema.Field.PREPARE_PLAN_START_TIME,
        Timestamp.ofEpochMilli(SessionState.getPerfLogger().getEndTime(PerfLogger.COMPILE), ZoneId.systemDefault()),
        record,
        fieldsValidated);
    compareValue(Schema.Field.GET_SESSION_DURATION,
        SessionState.getPerfLogger().getEndTime(PerfLogger.TEZ_GET_SESSION) - SessionState.getPerfLogger()
            .getStartTime(PerfLogger.TEZ_GET_SESSION), record,
        fieldsValidated);
    compareValue(Schema.Field.GET_SESSION_START_TIME,
        Timestamp.ofEpochMilli(SessionState.getPerfLogger().getStartTime(PerfLogger.TEZ_GET_SESSION),
            ZoneId.systemDefault()), record,
        fieldsValidated);
    compareValue(Schema.Field.EXECUTION_DURATION,
        SessionState.getPerfLogger().getEndTime(PerfLogger.TEZ_RUN_DAG) - SessionState.getPerfLogger()
            .getStartTime(PerfLogger.TEZ_RUN_DAG), record,
        fieldsValidated);
    compareValue(Schema.Field.EXECUTION_START_TIME,
        Timestamp.ofEpochMilli(SessionState.getPerfLogger().getStartTime(PerfLogger.TEZ_RUN_DAG),
            ZoneId.systemDefault()), record,
        fieldsValidated);

    compareValue(Schema.Field.FAILURE_REASON, DummyRecord.FAILURE_REASON, record,
        fieldsValidated);
    compareValue(Schema.Field.NUM_ROWS_FETCHED, DummyRecord.NUM_ROWS_FETCHED, record,
        fieldsValidated);
    compareValue(Schema.Field.PLAN, DummyRecord.PLAN, record, fieldsValidated);
    compareValue(Schema.Field.USED_TABLES, String.join(",",
        DummyRecord.USED_TABLES), record, fieldsValidated);
    compareValue(Schema.Field.EXEC_SUMMARY, DummyRecord.EXEC_SUMMARY, record,
        fieldsValidated);
    compareValue(Schema.Field.CONFIGURATION_OPTIONS_CHANGED,
        Joiner.on(";").withKeyValueSeparator("=").join(DummyRecord.CONFIGURATION_OPTIONS_CHANGED),
        record, fieldsValidated);
    compareValue(Schema.Field.TOTAL_LAUNCHED_TASKS,
        DummyRecord.TOTAL_LAUNCHED_TASKS, record, fieldsValidated);
    compareValue(Schema.Field.NUM_SUCCEEDED_TASKS,
        DummyRecord.NUM_SUCCEEDED_TASKS, record, fieldsValidated);
    compareValue(Schema.Field.NUM_KILLED_TASKS,
        DummyRecord.NUM_KILLED_TASKS, record, fieldsValidated);
    compareValue(Schema.Field.NUM_FAILED_TASKS,
        DummyRecord.NUM_FAILED_TASKS, record, fieldsValidated);
    compareValue(Schema.Field.TASK_DURATION_MILLIS,
        DummyRecord.TASK_DURATION_MILLIS, record, fieldsValidated);
    compareValue(Schema.Field.NODE_USED_COUNT,
        DummyRecord.NODE_USED_COUNT, record, fieldsValidated);
    compareValue(Schema.Field.NODE_TOTAL_COUNT,
        DummyRecord.NODE_TOTAL_COUNT, record, fieldsValidated);
    compareValue(Schema.Field.REDUCE_INPUT_GROUPS,
        DummyRecord.REDUCE_INPUT_GROUPS, record, fieldsValidated);
    compareValue(Schema.Field.REDUCE_INPUT_RECORDS,
        DummyRecord.REDUCE_INPUT_RECORDS, record, fieldsValidated);
    compareValue(Schema.Field.SPILLED_RECORDS,
        DummyRecord.SPILLED_RECORDS, record, fieldsValidated);
    compareValue(Schema.Field.NUM_SHUFFLED_INPUTS,
        DummyRecord.NUM_SHUFFLED_INPUTS, record, fieldsValidated);
    compareValue(Schema.Field.NUM_FAILED_SHUFFLE_INPUTS,
        DummyRecord.NUM_FAILED_SHUFFLE_INPUTS, record, fieldsValidated);
    compareValue(Schema.Field.INPUT_RECORDS_PROCESSED,
        DummyRecord.INPUT_RECORDS_PROCESSED, record, fieldsValidated);
    compareValue(Schema.Field.INPUT_SPLIT_LENGTH_BYTES,
        DummyRecord.INPUT_SPLIT_LENGTH_BYTES, record, fieldsValidated);
    compareValue(Schema.Field.OUTPUT_RECORDS,
        DummyRecord.OUTPUT_RECORDS, record, fieldsValidated);
    compareValue(Schema.Field.OUTPUT_BYTES_PHYSICAL,
        DummyRecord.OUTPUT_BYTES_PHYSICAL, record, fieldsValidated);
    compareValue(Schema.Field.SHUFFLE_CHUNK_COUNT,
        DummyRecord.SHUFFLE_CHUNK_COUNT, record, fieldsValidated);
    compareValue(Schema.Field.SHUFFLE_BYTES,
        DummyRecord.SHUFFLE_BYTES, record, fieldsValidated);
    compareValue(Schema.Field.SHUFFLE_BYTES_DISK_DIRECT,
        DummyRecord.SHUFFLE_BYTES_DISK_DIRECT, record, fieldsValidated);
    compareValue(Schema.Field.SHUFFLE_PHASE_TIME,
        DummyRecord.SHUFFLE_PHASE_TIME, record, fieldsValidated);
    compareValue(Schema.Field.MERGE_PHASE_TIME,
        DummyRecord.MERGE_PHASE_TIME, record, fieldsValidated);

    if (fieldsValidated.get() != FIELDS_TO_BE_VALIDATED) {
      Assert.fail(
          "Less (or more?) fields have been validated than expected: all: " + ALL_FIELDS + ", expected: "
              + FIELDS_TO_BE_VALIDATED + ", actual: " + fieldsValidated.get());
    }
  }

  // This method compares a single field in the record to the original value (which is expected to be in the record)
  private void compareValue(Schema.Field field, Object originalValue, Record record,
      AtomicInteger fieldsValidated) {
    Object valueInRecord = record.get(field);
    Assert.assertEquals(field + " values are not equal", originalValue, valueInRecord);
    fieldsValidated.incrementAndGet();
  }


  @Test
  public void testBatchSizeBasedFlush() {
    HiveConf conf = getHiveConfDummyRepository();
    Record record = new Record();
    // make sure we don't flush according to estimated memory size
    conf.setIntVar(HiveConf.ConfVars.HIVE_QUERY_HISTORY_MAX_MEMORY_BYTES, 1000000);
    // flush after the third record
    conf.setIntVar(HiveConf.ConfVars.HIVE_QUERY_HISTORY_BATCH_SIZE, 3);

    QueryHistoryServiceForTest service = new QueryHistoryServiceForTest(conf, serviceContext);

    Assert.assertFalse("Call to flush is not expected here (no records)",
        service.needToFlush());

    service.addRecordDirectly(record);
    Assert.assertFalse("Call to flush is not expected here (1 record)",
        service.needToFlush());

    service.addRecordDirectly(record);
    Assert.assertFalse("Call to flush is not expected here (2 records)",
        service.needToFlush());

    service.addRecordDirectly(record);
    Assert.assertTrue("Call to flush is expected here (3 records == configured limit)",
        service.needToFlush());
  }

  @Test
  public void testMemoryConsumptionBasedFlush() {
    HiveConf conf = getHiveConfDummyRepository();
    Record record = new Record();
    long recordSize = record.getEstimatedSizeInMemoryBytes();
    // make sure we don't flush according to batch size
    conf.setIntVar(HiveConf.ConfVars.HIVE_QUERY_HISTORY_BATCH_SIZE, 1000);
    // flush after the second record (2 * recordSize > recordSize + 1)
    conf.setIntVar(HiveConf.ConfVars.HIVE_QUERY_HISTORY_MAX_MEMORY_BYTES, (int)recordSize + 1);

    QueryHistoryServiceForTest service = new QueryHistoryServiceForTest(conf, serviceContext);

    Assert.assertFalse("Call to flush is not expected here (no records)",
        service.needToFlush());

    // add the record once: nothing is expected to happen
    service.addRecordDirectly(record);
    Assert.assertFalse("Call to flush is not expected here (1 record, didn't reach either of the limits)",
        service.needToFlush());

    // add the record again (same size): needToFlush is supposed to return true
    service.addRecordDirectly(record);
    Assert.assertTrue("Call to flush is expected here (memory size exceeded)",
        service.needToFlush());
  }

  @Test
  public void testPeriodicFlush() throws Exception {
    HiveConf conf = getHiveConfDummyRepository();
    Record record = new Record();
    // make sure we don't flush according to batch size
    conf.setIntVar(HiveConf.ConfVars.HIVE_QUERY_HISTORY_BATCH_SIZE, Integer.MAX_VALUE);
    // makre sure we don't flush according to batch size
    conf.setIntVar(HiveConf.ConfVars.HIVE_QUERY_HISTORY_MAX_MEMORY_BYTES, Integer.MAX_VALUE);
    // flush as soon as possible for testing purposes
    conf.setIntVar(HiveConf.ConfVars.HIVE_QUERY_HISTORY_FLUSH_INTERVAL_SECONDS, 1);

    QueryHistoryServiceForTest service = new QueryHistoryServiceForTest(conf, serviceContext);
    service.start();

    // add the record once: nothing is expected to happen
    service.addRecordDirectly(record);

    // The queue size is most likely 1 at this point, but let's skip this pre-assertion to avoid flakiness
    // (e.g., if the periodic check happens to run in the meantime).
    Thread.sleep(5000);

    Assert.assertEquals("Queue should have been flushed by the periodic check", 0, service.getQueue().size());
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
    conf.setIntVar(HiveConf.ConfVars.HIVE_QUERY_HISTORY_BATCH_SIZE, 0);
    return conf;
  }

  static class DummyQueryHistoryRepository implements QueryHistoryRepository {
    Record record = null;

    @Override
    public void init(HiveConf conf, Schema schema) {
      LOG.info("DummyQueryHistoryRepository init");
    }

    @Override
    public void flush(Queue<Record> records) {
      LOG.info("DummyQueryHistoryRepository to flush {} records", records.size());
      // saving for unit test purposes
      this.record = records.poll();
      records.clear();
    }
  }

  static class QueryHistoryServiceForTest extends QueryHistoryService {
    public QueryHistoryServiceForTest(HiveConf conf, ServiceContext serviceContext) {
      super(conf, serviceContext);
    }

    public void addRecordDirectly(Record record) {
      getQueue().add(record);
    }

    public Queue<Record> getQueue() {
      return queryHistoryQueue;
    }
  }
}
