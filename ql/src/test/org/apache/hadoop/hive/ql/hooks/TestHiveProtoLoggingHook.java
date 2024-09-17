/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.hooks;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.mr.ExecDriver;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.hooks.HiveProtoLoggingHook.EventLogger;
import org.apache.hadoop.hive.ql.hooks.HiveProtoLoggingHook.EventType;
import org.apache.hadoop.hive.ql.hooks.HiveProtoLoggingHook.ExecutionMode;
import org.apache.hadoop.hive.ql.hooks.HiveProtoLoggingHook.OtherInfoType;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.hooks.proto.HiveHookEvents.HiveHookEventProto;
import org.apache.hadoop.hive.ql.hooks.proto.HiveHookEvents.MapFieldEntry;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.history.logging.proto.DatePartitionedLogger;
import org.apache.tez.dag.history.logging.proto.ProtoMessageReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;


public class TestHiveProtoLoggingHook {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private HiveConf conf;
  private HookContext context;
  private String tmpFolder;

  @Before
  public void setup() throws Exception {
    conf = new HiveConf();
    conf.set(HiveConf.ConfVars.LLAP_DAEMON_QUEUE_NAME.varname, "llap_queue");
    conf.set(HiveConf.ConfVars.HIVE_PROTO_EVENTS_QUEUE_CAPACITY.varname, "3");
    conf.set(MRJobConfig.QUEUE_NAME, "mr_queue");
    conf.set(TezConfiguration.TEZ_QUEUE_NAME, "tez_queue");
    tmpFolder = folder.newFolder().getAbsolutePath();
    conf.setVar(HiveConf.ConfVars.HIVE_PROTO_EVENTS_BASE_PATH, tmpFolder);
    QueryState state = new QueryState.Builder().withHiveConf(conf).build();
    state.setCommandType(HiveOperation.QUERY);
    @SuppressWarnings("serial")
    QueryPlan queryPlan = new QueryPlan(HiveOperation.QUERY) {};
    queryPlan.setQueryId("test_queryId");
    queryPlan.setQueryStartTime(1234L);
    queryPlan.setQueryString("SELECT * FROM t WHERE i > 10");
    queryPlan.setRootTasks(new ArrayList<>());
    queryPlan.setInputs(new HashSet<>());
    queryPlan.setOutputs(new HashSet<>());

    PerfLogger perf = PerfLogger.getPerfLogger(conf, true);
    context = new HookContext(queryPlan, state, null, "test_user", "192.168.10.10",
        "hive_addr", "test_op_id", "test_session_id", "test_thread_id", true, perf, null);
  }

  @Test
  public void testPreEventLog() throws Exception {
    context.setHookType(HookType.PRE_EXEC_HOOK);
    EventLogger evtLogger = new EventLogger(conf, SystemClock.getInstance());
    evtLogger.handle(context);
    evtLogger.shutdown();

    HiveHookEventProto event = loadEvent(conf, tmpFolder);

    Assert.assertEquals(EventType.QUERY_SUBMITTED.name(), event.getEventType());
    Assert.assertEquals(1234L, event.getTimestamp());
    Assert.assertEquals(System.getProperty("user.name"), event.getUser());
    Assert.assertEquals("test_user", event.getRequestUser());
    Assert.assertEquals("test_queryId", event.getHiveQueryId());
    Assert.assertEquals("test_op_id", event.getOperationId());
    Assert.assertEquals(ExecutionMode.NONE.name(), event.getExecutionMode());
    Assert.assertFalse(event.hasQueue());

    assertOtherInfo(event, OtherInfoType.TEZ, Boolean.FALSE.toString());
    assertOtherInfo(event, OtherInfoType.MAPRED, Boolean.FALSE.toString());
    assertOtherInfo(event, OtherInfoType.CLIENT_IP_ADDRESS, "192.168.10.10");
    assertOtherInfo(event, OtherInfoType.SESSION_ID, "test_session_id");
    assertOtherInfo(event, OtherInfoType.THREAD_NAME, "test_thread_id");
    assertOtherInfo(event, OtherInfoType.HIVE_INSTANCE_TYPE, "HS2");
    assertOtherInfo(event, OtherInfoType.HIVE_ADDRESS, "hive_addr");
    assertOtherInfo(event, OtherInfoType.CONF, null);
    assertOtherInfo(event, OtherInfoType.QUERY, null);
  }

  @Test
  public void testNonPartionedTable() throws Exception {
    testTablesWritten(new WriteEntity(newTable(false), WriteEntity.WriteType.INSERT), false);
  }

  @Test
  public void testPartitionedTable() throws Exception {
    testTablesWritten(addPartitionOutput(newTable(true), WriteEntity.WriteType.INSERT), true);
  }

  @Test
  public void testQueueLogs() throws Exception {
    context.setHookType(HookType.PRE_EXEC_HOOK);
    EventLogger evtLogger = new EventLogger(conf, SystemClock.getInstance());

    // This makes it MR task
    context.getQueryPlan().getRootTasks().add(new ExecDriver());
    evtLogger.handle(context);

    // This makes it Tez task
    MapWork mapWork = new MapWork();
    TezWork tezWork = new TezWork("test_queryid");
    tezWork.add(mapWork);
    TezTask task = new TezTask();
    task.setId("id1");
    task.setWork(tezWork);
    context.getQueryPlan().getRootTasks().add(task);
    context.getQueryPlan().getRootTasks().add(new TezTask());
    evtLogger.handle(context);

    // This makes it llap task
    mapWork.setLlapMode(true);
    evtLogger.handle(context);

    evtLogger.shutdown();

    List<ProtoMessageReader<HiveHookEventProto>> readers = getTestReader(conf, tmpFolder);
    Assert.assertEquals(1, readers.size());
    ProtoMessageReader<HiveHookEventProto> reader = readers.get(0);

    HiveHookEventProto event = reader.readEvent();
    Assert.assertNotNull(event);
    Assert.assertEquals(ExecutionMode.MR.name(), event.getExecutionMode());
    Assert.assertEquals(event.getQueue(), "mr_queue");

    event = reader.readEvent();
    Assert.assertNotNull(event);
    Assert.assertEquals(ExecutionMode.TEZ.name(), event.getExecutionMode());
    Assert.assertEquals(event.getQueue(), "tez_queue");

    event = reader.readEvent();
    Assert.assertNotNull(event);
    Assert.assertEquals(ExecutionMode.LLAP.name(), event.getExecutionMode());
    Assert.assertEquals(event.getQueue(), "llap_queue");
  }

  @org.junit.Ignore("might fail intermittently")
  @Test
  public void testDropsEventWhenQueueIsFull() throws Exception {
    EventLogger evtLogger = new EventLogger(conf, SystemClock.getInstance());
    context.setHookType(HookType.PRE_EXEC_HOOK);
    evtLogger.handle(context);
    evtLogger.handle(context);
    evtLogger.handle(context);
    evtLogger.handle(context);
    evtLogger.shutdown();
    List<ProtoMessageReader<HiveHookEventProto>> readers = getTestReader(conf, tmpFolder);
    Assert.assertEquals(1, readers.size());
    ProtoMessageReader<HiveHookEventProto> reader = readers.get(0);
    reader.readEvent();
    reader.readEvent();
    reader.readEvent();
    try {
      reader.readEvent();
      Assert.fail("Expected 3 events due to queue capacity limit, got 4.");
    } catch (EOFException expected) {}
  }

  @Test
  public void testPreAndPostEventBoth() throws Exception {
    context.setHookType(HookType.PRE_EXEC_HOOK);
    EventLogger evtLogger = new EventLogger(conf, SystemClock.getInstance());
    evtLogger.handle(context);
    context.setHookType(HookType.POST_EXEC_HOOK);
    evtLogger.handle(context);
    evtLogger.shutdown();

    List<ProtoMessageReader<HiveHookEventProto>> readers = getTestReader(conf, tmpFolder);
    Assert.assertEquals(1, readers.size());
    ProtoMessageReader<HiveHookEventProto> reader = readers.get(0);
    HiveHookEventProto event = reader.readEvent();
    Assert.assertNotNull("Pre hook event not found", event);
    Assert.assertEquals(EventType.QUERY_SUBMITTED.name(), event.getEventType());

    event = reader.readEvent();
    Assert.assertNotNull("Post hook event not found", event);
    Assert.assertEquals(EventType.QUERY_COMPLETED.name(), event.getEventType());
  }

  @Test
  public void testPostEventLog() throws Exception {
    context.setHookType(HookType.POST_EXEC_HOOK);
    context.getPerfLogger().perfLogBegin("test", "LogTest");
    context.getPerfLogger().perfLogEnd("test", "LogTest");

    EventLogger evtLogger = new EventLogger(conf, SystemClock.getInstance());
    evtLogger.handle(context);
    evtLogger.shutdown();

    HiveHookEventProto event = loadEvent(conf, tmpFolder);
    Assert.assertEquals(EventType.QUERY_COMPLETED.name(), event.getEventType());
    Assert.assertEquals(System.getProperty("user.name"), event.getUser());
    Assert.assertEquals("test_user", event.getRequestUser());
    Assert.assertEquals("test_queryId", event.getHiveQueryId());
    Assert.assertEquals("test_op_id", event.getOperationId());

    assertOtherInfo(event, OtherInfoType.STATUS, Boolean.TRUE.toString());
    assertOtherInfo(event, OtherInfoType.QUERY_TYPE, HiveOperation.QUERY.toString());
    String val = findOtherInfo(event, OtherInfoType.PERF);
    Map<String, Long> map = new ObjectMapper().readValue(val,
        new TypeReference<Map<String, Long>>() {});
    // This should be really close to zero.
    Assert.assertTrue("Expected LogTest in PERF", map.get("LogTest") < 100);
  }

  @Test
  public void testFailureEventLog() throws Exception {
    context.setHookType(HookType.ON_FAILURE_HOOK);
    context.setErrorMessage("test_errormessage");

    EventLogger evtLogger = new EventLogger(conf, SystemClock.getInstance());
    evtLogger.handle(context);
    evtLogger.shutdown();

    HiveHookEventProto event = loadEvent(conf, tmpFolder);
    Assert.assertEquals(EventType.QUERY_COMPLETED.name(), event.getEventType());
    Assert.assertEquals(System.getProperty("user.name"), event.getUser());
    Assert.assertEquals("test_user", event.getRequestUser());
    Assert.assertEquals("test_queryId", event.getHiveQueryId());
    Assert.assertEquals("test_op_id", event.getOperationId());

    assertOtherInfo(event, OtherInfoType.STATUS, Boolean.FALSE.toString());
    assertOtherInfo(event, OtherInfoType.ERROR_MESSAGE, "test_errormessage");
    assertOtherInfo(event, OtherInfoType.PERF, null);
  }

  @Test
  public void testRolloverFiles() throws Exception {
    long waitTime = 100;
    context.setHookType(HookType.PRE_EXEC_HOOK);
    conf.setTimeDuration(ConfVars.HIVE_PROTO_EVENTS_ROLLOVER_CHECK_INTERVAL.varname, waitTime,
        TimeUnit.MICROSECONDS);
    Path path = new Path(tmpFolder);
    FileSystem fs = path.getFileSystem(conf);
    AtomicLong time = new AtomicLong();
    EventLogger evtLogger = new EventLogger(conf, () -> time.get());
    evtLogger.handle(context);
    int statusLen = 0;
    // Loop to ensure that we give some grace for scheduling issues.
    for (int i = 0; i < 3; ++i) {
      Thread.sleep(waitTime + 100);
      statusLen = fs.listStatus(path).length;
      if (statusLen > 0) {
        break;
      }
    }
    Assert.assertEquals(1, statusLen);

    // Move to next day and ensure a new file gets created.
    time.set(24 * 60 * 60 * 1000 + 1000);
    for (int i = 0; i < 3; ++i) {
      Thread.sleep(waitTime + 100);
      statusLen = fs.listStatus(path).length;
      if (statusLen > 1) {
        break;
      }
    }
    Assert.assertEquals(2, statusLen);
  }

  public static List<ProtoMessageReader<HiveHookEventProto>> getTestReader(HiveConf conf, String tmpFolder)
      throws IOException {
    Path path = new Path(tmpFolder);
    FileSystem fs = path.getFileSystem(conf);
    FileStatus[] folderStatuses = fs.listStatus(path);
    DatePartitionedLogger<HiveHookEventProto> logger = new DatePartitionedLogger<>(
        HiveHookEventProto.PARSER, path, conf, SystemClock.getInstance());
    List<ProtoMessageReader<HiveHookEventProto>> readers = new ArrayList<>();
    for (FileStatus folderStatus : folderStatuses) {
      FileStatus[] status = fs.listStatus(folderStatus.getPath());
      Assert.assertEquals(1, status.length);
      readers.add(logger.getReader(status[0].getPath()));
    }
    return readers;
  }

  private HiveHookEventProto loadEvent(HiveConf conf, String tmpFolder) throws IOException {
    List<ProtoMessageReader<HiveHookEventProto>> readers = getTestReader(conf, tmpFolder);
    Assert.assertEquals(1, readers.size());
    ProtoMessageReader<HiveHookEventProto> reader = readers.get(0);
    HiveHookEventProto event = reader.readEvent();
    Assert.assertNotNull(event);
    return event;
  }

  private String findOtherInfo(HiveHookEventProto event, OtherInfoType key) {
    for (MapFieldEntry otherInfo : event.getOtherInfoList()) {
      if (otherInfo.getKey().equals(key.name())) {
        return otherInfo.getValue();
      }
    }
    Assert.fail("Cannot find key " + key);
    return null;
  }

  private void assertOtherInfo(HiveHookEventProto event, OtherInfoType key, String value) {
    String val = findOtherInfo(event, key);
    if (value != null) {
      Assert.assertEquals(value, val);
    }
  }

  private void testTablesWritten(WriteEntity we, boolean isPartitioned) throws Exception {
    String query = isPartitioned ?
            "insert into test_partition partition(dt = '20220102', lable = 'test1') values('20220103', 'banana');" :
            "insert into default.testTable1 values('ab')";
    HashSet<WriteEntity> tableWritten = new HashSet<>();
    tableWritten.add(we);
    QueryState state = new QueryState.Builder().withHiveConf(conf).build();
    @SuppressWarnings("serial")
    QueryPlan queryPlan = new QueryPlan(HiveOperation.QUERY) {
    };
    queryPlan.setQueryId("test_queryId");
    queryPlan.setQueryStartTime(1234L);
    queryPlan.setQueryString(query);
    queryPlan.setRootTasks(new ArrayList<>());
    queryPlan.setInputs(new HashSet<>());
    queryPlan.setOutputs(tableWritten);
    PerfLogger perf = PerfLogger.getPerfLogger(conf, true);
    HookContext ctx = new HookContext(queryPlan, state, null, "test_user", "192.168.10.11",
            "hive_addr", "test_op_id", "test_session_id", "test_thread_id", true, perf, null);

    ctx.setHookType(HookType.PRE_EXEC_HOOK);
    EventLogger evtLogger = new EventLogger(conf, SystemClock.getInstance());
    evtLogger.handle(ctx);
    evtLogger.shutdown();

    HiveHookEventProto event = loadEvent(conf, tmpFolder);

    Assert.assertEquals(EventType.QUERY_SUBMITTED.name(), event.getEventType());
    Assert.assertEquals(we.getTable().getFullyQualifiedName(), event.getTablesWritten(0));
  }

  private Table newTable(boolean isPartitioned) {
    Table t = new Table("default", "testTable");
    if (isPartitioned) {
      FieldSchema fs = new FieldSchema();
      fs.setName("version");
      fs.setType("String");
      List<FieldSchema> partCols = new ArrayList<FieldSchema>(1);
      partCols.add(fs);
      t.setPartCols(partCols);
    }
    Map<String, String> tblProps = t.getParameters();
    if (tblProps == null) {
      tblProps = new HashMap<>();
    }
    tblProps.put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "true");
    t.setParameters(tblProps);
    return t;
  }

  private WriteEntity addPartitionOutput(Table t, WriteEntity.WriteType writeType) throws Exception {
    Map<String, String> partSpec = new HashMap<String, String>();
    partSpec.put("version", Integer.toString(1));
    Partition p = new Partition(t, partSpec, new Path("/dev/null"));
    WriteEntity we = new WriteEntity(p, writeType);
    return we;
  }

}
