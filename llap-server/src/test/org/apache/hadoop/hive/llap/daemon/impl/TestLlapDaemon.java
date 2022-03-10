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
package org.apache.hadoop.hive.llap.daemon.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.LlapDaemonInfo;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.metrics.LlapMetricsSystem;
import org.apache.hadoop.hive.llap.metrics.MetricsUtils;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Fields;
import org.mockito.internal.util.reflection.InstanceField;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.lang.Integer.parseInt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class TestLlapDaemon {

  private static final String[] METRICS_SOURCES = new String[]{
      "JvmMetrics",
      "LlapDaemonExecutorMetrics-" + MetricsUtils.getHostName(),
      "LlapDaemonJvmMetrics-" + MetricsUtils.getHostName(),
      MetricsUtils.METRICS_PROCESS_NAME
  };

  private Configuration hiveConf = new HiveConf();

  @Mock
  private LlapRegistryService mockRegistry;

  @Captor
  private ArgumentCaptor<Iterable<Map.Entry<String, String>>> captor;

  private LlapDaemon daemon;

  @Before
  public void setUp() {
    initMocks(this);
    HiveConf.setVar(hiveConf, HiveConf.ConfVars.LLAP_DAEMON_SERVICE_HOSTS, "@llap");
    HiveConf.setVar(hiveConf, HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM, "localhost");

    String[] localDirs = new String[1];
    LlapDaemonInfo.initialize("testDaemon", hiveConf);
    daemon = new LlapDaemon(hiveConf, 1, LlapDaemon.getTotalHeapSize(), false, false,
            -1, localDirs, 0, false, 0,0, 0, -1, "TestLlapDaemon");
  }

  @After
  public void tearDown() {
    MetricsSystem ms = LlapMetricsSystem.instance();
    for (String mSource : METRICS_SOURCES) {
      ms.unregisterSource(mSource);
    }
    daemon.shutdown();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEnforceProperNumberOfIOThreads() throws IOException {
    Configuration thisHiveConf = new HiveConf();
    HiveConf.setVar(thisHiveConf, HiveConf.ConfVars.LLAP_DAEMON_SERVICE_HOSTS, "@llap");
    HiveConf.setIntVar(thisHiveConf, HiveConf.ConfVars.LLAP_DAEMON_NUM_EXECUTORS, 4);
    HiveConf.setIntVar(thisHiveConf, HiveConf.ConfVars.LLAP_IO_THREADPOOL_SIZE, 3);

    LlapDaemon thisDaemon = new LlapDaemon(thisHiveConf, 1, LlapDaemon.getTotalHeapSize(), false, false,
            -1, new String[1], 0, false, 0,0, 0, -1, "TestLlapDaemon");
    thisDaemon.close();
  }

  @Test
  public void testUpdateRegistration() throws IOException {
    // Given
    int enabledExecutors = 0;
    int enabledQueue = 2;
    trySetMock(daemon, LlapRegistryService.class, mockRegistry);

    // When
    daemon.setCapacity(LlapDaemonProtocolProtos.SetCapacityRequestProto.newBuilder()
        .setQueueSize(enabledQueue)
        .setExecutorNum(enabledExecutors)
        .build());
    verify(mockRegistry).updateRegistration(captor.capture());

    // Then
    Map<String, String> attributes = StreamSupport.stream(captor.getValue().spliterator(), false)
        .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));

    assertTrue(attributes.containsKey(LlapRegistryService.LLAP_DAEMON_NUM_ENABLED_EXECUTORS));
    assertTrue(attributes.containsKey(LlapRegistryService.LLAP_DAEMON_TASK_SCHEDULER_ENABLED_WAIT_QUEUE_SIZE));
    assertEquals(enabledQueue,
        parseInt(attributes.get(LlapRegistryService.LLAP_DAEMON_TASK_SCHEDULER_ENABLED_WAIT_QUEUE_SIZE)));
    assertEquals(enabledExecutors,
        parseInt(attributes.get(LlapRegistryService.LLAP_DAEMON_NUM_ENABLED_EXECUTORS)));

  }

  static <T> void trySetMock(Object o, Class<T> clazz, T mock) {
    List<InstanceField> instanceFields = Fields
        .allDeclaredFieldsOf(o)
        .filter(instanceField -> !clazz.isAssignableFrom(instanceField.jdkField().getType()))
        .instanceFields();
    if (instanceFields.size() != 1) {
      throw new RuntimeException("Mocking is only supported, if only one field is assignable from the given class.");
    }
    InstanceField instanceField = instanceFields.get(0);
    instanceField.set(mock);
  }
}
