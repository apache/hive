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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.internal.util.reflection.Fields;
import org.mockito.internal.util.reflection.InstanceField;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class TestLlapDaemon {

  private static final String[] METRICS_SOURCES = new String[]{
    "JvmMetrics",
    "LlapDaemonExecutorMetrics-" + MetricsUtils.getHostName(),
    "LlapDaemonJvmMetrics-" + MetricsUtils.getHostName(),
    MetricsUtils.METRICS_PROCESS_NAME
  };

  @Spy
  private Configuration mockConf = new HiveConf();

  @Mock
  private LlapRegistryService mockRegistry;

  @Captor
  private ArgumentCaptor<Iterable<Map.Entry<String, String>>> captor;

  private LlapDaemon daemon;

  @Before
  public void setUp() {
    initMocks(this);
    when(mockConf.getTrimmed(HiveConf.ConfVars.LLAP_DAEMON_SERVICE_HOSTS.varname, null)).thenReturn("@llap");
    when(mockConf.getTrimmed(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM.varname, "")).thenReturn("localhost");

    String[] localDirs = new String[1];
    LlapDaemonInfo.initialize("testDaemon", mockConf);
    daemon = new LlapDaemon(mockConf, 1, LlapDaemon.getTotalHeapSize(), false, false,
            -1, localDirs, 0, 0, 0, -1, "TestLlapDaemon");
  }

  @After
  public void tearDown() {
    MetricsSystem ms = LlapMetricsSystem.instance();
    for (String mSource : METRICS_SOURCES) {
      ms.unregisterSource(mSource);
    }
    daemon.shutdown();
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

    Assert.assertTrue(attributes.containsKey(LlapRegistryService.LLAP_DAEMON_NUM_ENABLED_EXECUTORS));
    Assert.assertTrue(attributes.containsKey(LlapRegistryService.LLAP_DAEMON_TASK_SCHEDULER_ENABLED_WAIT_QUEUE_SIZE));
    Assert.assertEquals(enabledQueue,
      Integer.parseInt(attributes.get(LlapRegistryService.LLAP_DAEMON_TASK_SCHEDULER_ENABLED_WAIT_QUEUE_SIZE)));
    Assert.assertEquals(enabledExecutors,
      Integer.parseInt(attributes.get(LlapRegistryService.LLAP_DAEMON_NUM_ENABLED_EXECUTORS)));

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
