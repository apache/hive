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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;

import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
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
public class Metrics {

  /**
   * MetricsScope : A class that encapsulates an idea of a metered scope.
   * Instantiating a named scope and then closing it exposes two counters:
   *   (i) a "number of calls" counter ( &lt;name&gt;.n ), and
   *  (ii) a "number of msecs spent between scope open and close" counter. ( &lt;name&gt;.t)
   */
  public class MetricsScope {

    String name = null;
    boolean isOpen = false;
    Long startTime = null;
    String numCounter = null;
    String timeCounter = null;
    String avgTimeCounter = null;

    //disable default ctor - so that it can't be created without a name
    @SuppressWarnings("unused")
    private MetricsScope() {
    }

    /**
     * Instantiates a named scope - intended to only be called by Metrics, so locally scoped.
     * @param name - name of the variable
     * @throws IOException
     */
    MetricsScope(String name) throws IOException {
      this.name = name;
      this.numCounter = name + ".n";
      this.timeCounter = name + ".t";
      this.avgTimeCounter = name + ".avg_t";
      open();
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
          Long num = Metrics.incrementCounter(numCounter);
          Long time = Metrics.incrementCounter(timeCounter, endTime - startTime);
          if (num != null && time != null) {
            Metrics.set(avgTimeCounter, Double.valueOf(time.doubleValue() / num.doubleValue()));
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


  static MetricsMBean metrics = new MetricsMBeanImpl();

  static ThreadLocal<HashMap<String, MetricsScope>> threadLocalScopes
    = new ThreadLocal<HashMap<String,MetricsScope>>() {
    @Override
    protected synchronized HashMap<String,MetricsScope> initialValue() {
      return new HashMap<String,MetricsScope>();
    }
  };

  static boolean initialized = false;

  static Metrics m = new Metrics();

  public static void init() throws Exception {
    if (!initialized) {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName oname = new ObjectName(
        "org.apache.hadoop.hive.common.metrics:type=MetricsMBean");
      mbs.registerMBean(metrics, oname);
      initialized = true;
    }
  }

  public static Long incrementCounter(String name) throws IOException{
    if (!initialized) {
      return null;
    }
    return incrementCounter(name,Long.valueOf(1));
  }

  public static Long incrementCounter(String name, long increment) throws IOException{
    if (!initialized) {
      return null;
    }
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

  public static void set(String name, Object value) throws IOException{
    if (!initialized) {
      return;
    }
    synchronized(metrics) {
      metrics.put(name,value);
    }
  }

  public static Object get(String name) throws IOException{
    if (!initialized) {
      return null;
    }
    return metrics.get(name);
  }

  public static MetricsScope startScope(String name) throws IOException{
    if (!initialized) {
      return null;
    }
    if (threadLocalScopes.get().containsKey(name)) {
      threadLocalScopes.get().get(name).open();
    } else {
      threadLocalScopes.get().put(name, m.new MetricsScope(name));
    }
    return threadLocalScopes.get().get(name);
  }

  public static MetricsScope getScope(String name) throws IOException {
    if (!initialized) {
      return null;
    }
    if (threadLocalScopes.get().containsKey(name)) {
      return threadLocalScopes.get().get(name);
    } else {
      throw new IOException("No metrics scope named " + name);
    }
  }

  public static void endScope(String name) throws IOException{
    if (!initialized) {
      return;
    }
    if (threadLocalScopes.get().containsKey(name)) {
      threadLocalScopes.get().get(name).close();
    }
  }

}
