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
package org.apache.hadoop.hive.common.metrics;

import java.lang.management.ManagementFactory;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.management.Attribute;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestLegacyMetrics {

  private static final String scopeName = "foo";
  private static final long periodMs = 50L;
  private static LegacyMetrics metrics;

  @Before
  public void before() throws Exception {
    MetricsFactory.close();
    Configuration conf = new Configuration();
    conf.set(MetastoreConf.ConfVars.METRICS_CLASS.getHiveName(), LegacyMetrics.class.getCanonicalName());
    MetricsFactory.init(conf);
    metrics = (LegacyMetrics) MetricsFactory.getInstance();
  }

  @After
  public void after() throws Exception {
    MetricsFactory.close();
  }

  @Test
  public void testMetricsMBean() throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    final ObjectName oname = new ObjectName(
        "org.apache.hadoop.hive.common.metrics:type=MetricsMBean");
    MBeanInfo mBeanInfo = mbs.getMBeanInfo(oname);
    // check implementation class:
    assertEquals(MetricsMBeanImpl.class.getName(), mBeanInfo.getClassName());

    // check reset operation:
    MBeanOperationInfo[] oops = mBeanInfo.getOperations();
    boolean resetFound = false;
    for (MBeanOperationInfo op : oops) {
      if ("reset".equals(op.getName())) {
        resetFound = true;
        break;
      }
    }
    assertTrue(resetFound);

    // add metric with a non-null value:
    Attribute attr = new Attribute("fooMetric", Long.valueOf(-77));
    mbs.setAttribute(oname, attr);

    mBeanInfo = mbs.getMBeanInfo(oname);
    MBeanAttributeInfo[] attributeInfos = mBeanInfo.getAttributes();
    assertEquals(1, attributeInfos.length);
    boolean attrFound = false;
    for (MBeanAttributeInfo info : attributeInfos) {
      if ("fooMetric".equals(info.getName())) {
        assertEquals("java.lang.Long", info.getType());
        assertTrue(info.isReadable());
        assertTrue(info.isWritable());
        assertFalse(info.isIs());

        attrFound = true;
        break;
      }
    }
    assertTrue(attrFound);

    // check metric value:
    Object v = mbs.getAttribute(oname, "fooMetric");
    assertEquals(Long.valueOf(-77), v);

    // reset the bean:
    Object result = mbs.invoke(oname, "reset", new Object[0], new String[0]);
    assertNull(result);

    // the metric value must be zeroed:
    v = mbs.getAttribute(oname, "fooMetric");
    assertEquals(Long.valueOf(0), v);
  }

  @Test
  public void testScopeSingleThread() throws Exception {
    metrics.startStoredScope(scopeName);
    final LegacyMetrics.LegacyMetricsScope fooScope = (LegacyMetrics.LegacyMetricsScope) metrics.getStoredScope(scopeName);
    // the time and number counters become available only after the 1st
    // scope close:
    Long num = fooScope.getNumCounter();
    assertNull(num);

    Long time = fooScope.getTimeCounter();
    assertNull(time);

    assertSame(fooScope, metrics.getStoredScope(scopeName));
    Thread.sleep(periodMs+ 1);
    // 1st close:
    // closing of open scope should be ok:
    metrics.endStoredScope(scopeName);

    assertEquals(Long.valueOf(1), fooScope.getNumCounter());
    final long t1 = fooScope.getTimeCounter().longValue();
    assertTrue(t1 > periodMs);

    assertSame(fooScope, metrics.getStoredScope(scopeName));

   // opening allowed after closing:
    metrics.startStoredScope(scopeName);

    assertEquals(Long.valueOf(1), fooScope.getNumCounter());
    assertEquals(t1, fooScope.getTimeCounter().longValue());

    assertSame(fooScope, metrics.getStoredScope(scopeName));
    Thread.sleep(periodMs + 1);
    // Reopening (close + open) allowed in opened state:
    fooScope.reopen();

    assertEquals(Long.valueOf(2), fooScope.getNumCounter());
    assertTrue(fooScope.getTimeCounter().longValue() > 2 * periodMs);

    Thread.sleep(periodMs + 1);
    // 3rd close:
    fooScope.close();

    assertEquals(Long.valueOf(3), fooScope.getNumCounter());
    assertTrue(fooScope.getTimeCounter().longValue() > 3 * periodMs);
    Double avgT = (Double) metrics.get("foo.avg_t");
    assertTrue(avgT.doubleValue() > periodMs);
  }

  @Test
  public void testScopeConcurrency() throws Exception {
    metrics.startStoredScope(scopeName);
    LegacyMetrics.LegacyMetricsScope fooScope = (LegacyMetrics.LegacyMetricsScope) metrics.getStoredScope(scopeName);
    final int threads = 10;
    ExecutorService executorService = Executors.newFixedThreadPool(threads);
    for (int i=0; i<threads; i++) {
      final int n = i;
      executorService.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          testScopeImpl(n);
          return null;
        }
      });
    }
    executorService.shutdown();
    assertTrue(executorService.awaitTermination(periodMs * 3 * threads, TimeUnit.MILLISECONDS));

    fooScope = (LegacyMetrics.LegacyMetricsScope) metrics.getStoredScope(scopeName);
    assertEquals(Long.valueOf(3 * threads), fooScope.getNumCounter());
    assertTrue(fooScope.getTimeCounter().longValue() > 3 * periodMs * threads);
    Double avgT = (Double) metrics.get("foo.avg_t");
    assertTrue(avgT.doubleValue() > periodMs);
    metrics.endStoredScope(scopeName);
  }

  @Test
  public void testScopeIncorrectOpenOrder() throws Exception {
    metrics.startStoredScope(scopeName);
    LegacyMetrics.LegacyMetricsScope fooScope = (LegacyMetrics.LegacyMetricsScope) metrics.getStoredScope(scopeName);
    assertEquals(null, fooScope.getNumCounter());
    fooScope.close();
    assertEquals(Long.valueOf(1), fooScope.getNumCounter());

    for (int i=0; i<10; i++) {
      fooScope.open();
      fooScope.close();
    }
    // scope opened/closed 10 times
    assertEquals(Long.valueOf(11), fooScope.getNumCounter());

    for (int i=0; i<10; i++) {
      fooScope.open();
    }
    for (int i=0; i<10; i++) {
      fooScope.close();
    }
    // scope opened/closed once (multiple opens do not count)
    assertEquals(Long.valueOf(12), fooScope.getNumCounter());
  }

  void testScopeImpl(int n) throws Exception {
    metrics.startStoredScope(scopeName);
    final LegacyMetrics.LegacyMetricsScope fooScope = (LegacyMetrics.LegacyMetricsScope) metrics.getStoredScope(scopeName);

    assertSame(fooScope, metrics.getStoredScope(scopeName));
    Thread.sleep(periodMs+ 1);
    // 1st close:
    metrics.endStoredScope(scopeName); // closing of open scope should be ok.

    assertTrue(fooScope.getNumCounter().longValue() >= 1);
    final long t1 = fooScope.getTimeCounter().longValue();
    assertTrue(t1 > periodMs);

    assertSame(fooScope, metrics.getStoredScope(scopeName));

   // opening allowed after closing:
    metrics.startStoredScope(scopeName);

    assertTrue(fooScope.getNumCounter().longValue() >= 1);
    assertTrue(fooScope.getTimeCounter().longValue() >= t1);

    assertSame(fooScope, metrics.getStoredScope(scopeName));
    Thread.sleep(periodMs + 1);
    // Reopening (close + open) allowed in opened state:
    fooScope.reopen();

    assertTrue(fooScope.getNumCounter().longValue() >= 2);
    assertTrue(fooScope.getTimeCounter().longValue() > 2 * periodMs);

    Thread.sleep(periodMs + 1);
    // 3rd close:
    fooScope.close();

    assertTrue(fooScope.getNumCounter().longValue() >= 3);
    assertTrue(fooScope.getTimeCounter().longValue() > 3 * periodMs);
    Double avgT = (Double) metrics.get("foo.avg_t");
    assertTrue(avgT.doubleValue() > periodMs);
  }
}
