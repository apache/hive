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
package org.apache.hadoop.hive.common.metrics;

import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsScope;
import org.apache.hadoop.hive.common.metrics.common.MetricsVariable;
import org.apache.hadoop.hive.conf.HiveConf;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

/**
 * This class may eventually get superseded by org.apache.hadoop.hive.common.metrics2.Metrics.
 *
 * Metrics Subsystem  - allows exposure of a number of named parameters/counters
 *                      via jmx, intended to be used as a static subsystem
 *
 *                      Has a couple of primary ways it can be used:
 *                      (i) Using the set and get methods to set and get named parameters
 *                      (ii) Using the incrementCounter method to increment and set named
 *                      parameters in one go, rather than having to make a get and then a set.
 *                      (iii) Using the startScope and endScope methods to start and end
 *                      named "scopes" that record the number of times they've been
 *                      instantiated and amount of time(in milliseconds) spent inside
 *                      the scopes.
 */
public class LegacyMetrics implements Metrics {

  private LegacyMetrics() {
    // block
  }

  /**
   * MetricsScope : A class that encapsulates an idea of a metered scope.
   * Instantiating a named scope and then closing it exposes two counters:
   *   (i) a "number of calls" counter ( &lt;name&gt;.n ), and
   *  (ii) a "number of msecs spent between scope open and close" counter. ( &lt;name&gt;.t)
   */
  public static class LegacyMetricsScope implements MetricsScope {

    final LegacyMetrics metrics;

    final String name;
    final String numCounter;
    final String timeCounter;
    final String avgTimeCounter;

    private boolean isOpen = false;
    private Long startTime = null;

    /**
     * Instantiates a named scope - intended to only be called by Metrics, so locally scoped.
     * @param name - name of the variable
     * @throws IOException
     */
    private LegacyMetricsScope(String name, LegacyMetrics metrics) throws IOException {
      this.metrics = metrics;
      this.name = name;
      this.numCounter = name + ".n";
      this.timeCounter = name + ".t";
      this.avgTimeCounter = name + ".avg_t";
      open();
    }

    public Long getNumCounter() throws IOException {
      return (Long) metrics.get(numCounter);
    }

    public Long getTimeCounter() throws IOException {
      return (Long) metrics.get(timeCounter);
    }

    /**
     * Opens scope, and makes note of the time started, increments run counter
     * @throws IOException
     *
     */
    public void open() throws IOException {
      if (!isOpen) {
        isOpen = true;
        startTime = System.currentTimeMillis();
      } else {
        throw new IOException("Scope named " + name + " is not closed, cannot be opened.");
      }
    }

    /**
     * Closes scope, and records the time taken
     * @throws IOException
     */
    public void close() throws IOException {
      if (isOpen) {
        Long endTime = System.currentTimeMillis();
        synchronized(metrics) {
          Long num = metrics.incrementCounter(numCounter);
          Long time = metrics.incrementCounter(timeCounter, endTime - startTime);
          if (num != null && time != null) {
            metrics.set(avgTimeCounter, Double.valueOf(time.doubleValue() / num.doubleValue()));
          }
        }
      } else {
        throw new IOException("Scope named " + name + " is not open, cannot be closed.");
      }
      isOpen = false;
    }


    /**
     * Closes scope if open, and reopens it
     * @throws IOException
     */
    public void reopen() throws IOException {
      if(isOpen) {
        close();
      }
      open();
    }

  }

  private static final MetricsMBean metrics = new MetricsMBeanImpl();

  private static final ObjectName oname;
  static {
    try {
      oname = new ObjectName(
          "org.apache.hadoop.hive.common.metrics:type=MetricsMBean");
    } catch (MalformedObjectNameException mone) {
      throw new RuntimeException(mone);
    }
  }

  private static final ThreadLocal<HashMap<String, LegacyMetricsScope>> threadLocalScopes
    = new ThreadLocal<HashMap<String, LegacyMetricsScope>>() {
    @Override
    protected HashMap<String, LegacyMetricsScope> initialValue() {
      return new HashMap<String, LegacyMetricsScope>();
    }
  };

  public LegacyMetrics(HiveConf conf) throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    mbs.registerMBean(metrics, oname);
  }

  public Long incrementCounter(String name) throws IOException {
    return incrementCounter(name,Long.valueOf(1));
  }

  public Long incrementCounter(String name, long increment) throws IOException {
    Long value;
    synchronized(metrics) {
      if (!metrics.hasKey(name)) {
        value = Long.valueOf(increment);
        set(name, value);
      } else {
        value = ((Long)get(name)) + increment;
        set(name, value);
      }
    }
    return value;
  }

  public Long decrementCounter(String name) throws IOException{
    return decrementCounter(name, Long.valueOf(1));
  }

  public Long decrementCounter(String name, long decrement) throws IOException {
    Long value;
    synchronized(metrics) {
      if (!metrics.hasKey(name)) {
        value = Long.valueOf(decrement);
        set(name, -value);
      } else {
        value = ((Long)get(name)) - decrement;
        set(name, value);
      }
    }
    return value;
  }

  @Override
  public void addGauge(String name, MetricsVariable variable) {
    //Not implemented.
  }

  public void set(String name, Object value) throws IOException{
    metrics.put(name,value);
  }

  public Object get(String name) throws IOException{
    return metrics.get(name);
  }

  public void startStoredScope(String name) throws IOException{
    if (threadLocalScopes.get().containsKey(name)) {
      threadLocalScopes.get().get(name).open();
    } else {
      threadLocalScopes.get().put(name, new LegacyMetricsScope(name, this));
    }
  }

  public MetricsScope getStoredScope(String name) throws IOException {
    if (threadLocalScopes.get().containsKey(name)) {
      return threadLocalScopes.get().get(name);
    } else {
      throw new IOException("No metrics scope named " + name);
    }
  }

  public void endStoredScope(String name) throws IOException{
    if (threadLocalScopes.get().containsKey(name)) {
      threadLocalScopes.get().get(name).close();
    }
  }

  public MetricsScope createScope(String name) throws IOException {
    return new LegacyMetricsScope(name, this);
  }

  public void endScope(MetricsScope scope) throws IOException {
    ((LegacyMetricsScope) scope).close();
  }

  /**
   * Resets the static context state to initial.
   * Used primarily for testing purposes.
   *
   * Note that threadLocalScopes ThreadLocal is *not* cleared in this call.
   */
  public void close() throws Exception {
    synchronized (metrics) {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      if (mbs.isRegistered(oname)) {
        mbs.unregisterMBean(oname);
      }
      metrics.clear();
      threadLocalScopes.remove();
    }
  }
}
