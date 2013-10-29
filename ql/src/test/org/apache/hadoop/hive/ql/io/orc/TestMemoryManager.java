/**
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
package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Test;

import java.lang.management.ManagementFactory;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doubleThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Test the ORC memory manager.
 */
public class TestMemoryManager {
  private static final double ERROR = 0.000001;

  private static class NullCallback implements MemoryManager.Callback {
    public boolean checkMemory(double newScale) {
      return false;
    }
  }

  @Test
  public void testBasics() throws Exception {
    Configuration conf = new Configuration();
    MemoryManager mgr = new MemoryManager(conf);
    NullCallback callback = new NullCallback();
    long poolSize = mgr.getTotalMemoryPool();
    assertEquals(Math.round(ManagementFactory.getMemoryMXBean().
        getHeapMemoryUsage().getMax() * 0.5d), poolSize);
    assertEquals(1.0, mgr.getAllocationScale(), 0.00001);
    mgr.addWriter(new Path("p1"), 1000, callback);
    assertEquals(1.0, mgr.getAllocationScale(), 0.00001);
    mgr.addWriter(new Path("p1"), poolSize / 2, callback);
    assertEquals(1.0, mgr.getAllocationScale(), 0.00001);
    mgr.addWriter(new Path("p2"), poolSize / 2, callback);
    assertEquals(1.0, mgr.getAllocationScale(), 0.00001);
    mgr.addWriter(new Path("p3"), poolSize / 2, callback);
    assertEquals(0.6666667, mgr.getAllocationScale(), 0.00001);
    mgr.addWriter(new Path("p4"), poolSize / 2, callback);
    assertEquals(0.5, mgr.getAllocationScale(), 0.000001);
    mgr.addWriter(new Path("p4"), 3 * poolSize / 2, callback);
    assertEquals(0.3333333, mgr.getAllocationScale(), 0.000001);
    mgr.removeWriter(new Path("p1"));
    mgr.removeWriter(new Path("p2"));
    assertEquals(0.5, mgr.getAllocationScale(), 0.00001);
    mgr.removeWriter(new Path("p4"));
    assertEquals(1.0, mgr.getAllocationScale(), 0.00001);
  }

  @Test
  public void testConfig() throws Exception {
    Configuration conf = new Configuration();
    conf.set("hive.exec.orc.memory.pool", "0.9");
    MemoryManager mgr = new MemoryManager(conf);
    long mem =
        ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
    System.err.print("Memory = " + mem);
    long pool = mgr.getTotalMemoryPool();
    assertTrue("Pool too small: " + pool, mem * 0.899 < pool);
    assertTrue("Pool too big: " + pool, pool < mem * 0.901);
  }

  private static class DoubleMatcher extends BaseMatcher<Double> {
    final double expected;
    final double error;
    DoubleMatcher(double expected, double error) {
      this.expected = expected;
      this.error = error;
    }

    @Override
    public boolean matches(Object val) {
      double dbl = (Double) val;
      return Math.abs(dbl - expected) <= error;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("not sufficiently close to ");
      description.appendText(Double.toString(expected));
    }
  }

  private static DoubleMatcher closeTo(double value, double error) {
    return new DoubleMatcher(value, error);
  }

  @Test
  public void testCallback() throws Exception {
    Configuration conf = new Configuration();
    MemoryManager mgr = new MemoryManager(conf);
    long pool = mgr.getTotalMemoryPool();
    MemoryManager.Callback[] calls = new MemoryManager.Callback[20];
    for(int i=0; i < calls.length; ++i) {
      calls[i] = mock(MemoryManager.Callback.class);
      mgr.addWriter(new Path(Integer.toString(i)), pool/4, calls[i]);
    }
    // add enough rows to get the memory manager to check the limits
    for(int i=0; i < 10000; ++i) {
      mgr.addedRow();
    }
    for(int call=0; call < calls.length; ++call) {
      verify(calls[call], times(2))
          .checkMemory(doubleThat(closeTo(0.2, ERROR)));
    }
  }
}
