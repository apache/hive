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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.hooks.HiveProtoLoggingHook.EventLogger;
import org.apache.hadoop.hive.ql.hooks.HiveProtoLoggingHook.EventType;
import org.apache.hadoop.hive.ql.hooks.HiveProtoLoggingHook.OtherInfoType;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.hooks.proto.HiveHookEvents.HiveHookEventProto;
import org.apache.hadoop.hive.ql.hooks.proto.HiveHookEvents.MapFieldEntry;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.dag.history.logging.proto.DatePartitionedLogger;
import org.apache.tez.dag.history.logging.proto.ProtoMessageReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class TestHiveProtoLoggingHook {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private HiveConf conf;
  private HookContext context;
  private String tmpFolder;

  @Before
  public void setup() throws Exception {
    conf = new HiveConf();
    tmpFolder = folder.newFolder().getAbsolutePath();
    conf.set(HiveProtoLoggingHook.HIVE_EVENTS_BASE_PATH, tmpFolder);
    QueryState state = new QueryState.Builder().withHiveConf(conf).build();
    @SuppressWarnings("serial")
    QueryPlan queryPlan = new QueryPlan(HiveOperation.QUERY) {};
    queryPlan.setQueryId("test_queryId");
    queryPlan.setQueryStartTime(1234L);
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
    Assert.assertEquals("NONE", event.getExecutionMode());

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
  public void testPostEventLog() throws Exception {
    context.setHookType(HookType.POST_EXEC_HOOK);

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
    assertOtherInfo(event, OtherInfoType.PERF, null);
  }

  @Test
  public void testFailureEventLog() throws Exception {
    context.setHookType(HookType.ON_FAILURE_HOOK);

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
    assertOtherInfo(event, OtherInfoType.PERF, null);
  }

  private HiveHookEventProto loadEvent(HiveConf conf, String tmpFolder)
      throws IOException, FileNotFoundException {
    Path path = new Path(tmpFolder);
    FileSystem fs = path.getFileSystem(conf);
    FileStatus[] status = fs.listStatus(path);
    Assert.assertEquals(1, status.length);
    status = fs.listStatus(status[0].getPath());
    Assert.assertEquals(1, status.length);

    DatePartitionedLogger<HiveHookEventProto> logger = new DatePartitionedLogger<>(
        HiveHookEventProto.PARSER, path, conf, SystemClock.getInstance());
    ProtoMessageReader<HiveHookEventProto> reader = logger.getReader(status[0].getPath());
    HiveHookEventProto event = reader.readEvent();
    Assert.assertNotNull(event);
    return event;
  }

  private void assertOtherInfo(HiveHookEventProto event, OtherInfoType key, String value) {
    for (MapFieldEntry otherInfo : event.getOtherInfoList()) {
      if (otherInfo.getKey().equals(key.name())) {
        if (value != null) {
          Assert.assertEquals(value, otherInfo.getValue());
        }
        return;
      }
    }
    Assert.fail("Cannot find key: " + key);
  }
}
