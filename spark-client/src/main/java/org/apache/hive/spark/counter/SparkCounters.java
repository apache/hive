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
package org.apache.hive.spark.counter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * SparkCounters is used to collect Hive operator metric through Spark accumulator. There are few
 * limitation of Spark accumulator, like:
 * 1. accumulator should be created at Spark context side.
 * 2. Spark tasks can only increment metric count.
 * 3. User can only get accumulator value at Spark context side.
 * These Spark Counter API is designed to fit into Hive requirement, while with several access
 * restriction due to Spark accumulator previous mentioned:
 * 1. Counter should be created on driver side if it would be accessed in task.
 * 2. increment could only be invoked task side.
 * 3. Hive could only get Counter value at driver side.
 */
public class SparkCounters implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final Log LOG = LogFactory.getLog(SparkCounters.class);

  private Map<String, SparkCounterGroup> sparkCounterGroups;

  private final transient JavaSparkContext javaSparkContext;

  private SparkCounters() {
    this(null);
  }

  public SparkCounters(JavaSparkContext javaSparkContext) {
    this.javaSparkContext = javaSparkContext;
    this.sparkCounterGroups = new HashMap<String, SparkCounterGroup>();
  }

  public void createCounter(Enum<?> key) {
    createCounter(key.getDeclaringClass().getName(), key.name());
  }

  public void createCounter(String groupName, Enum<?> key) {
    createCounter(groupName, key.name(), 0L);
  }

  public void createCounter(String groupName, String counterName) {
    createCounter(groupName, counterName, 0L);
  }

  public void createCounter(String groupName, String counterName, long initValue) {
    getGroup(groupName).createCounter(counterName, initValue);
  }

  public void increment(Enum<?> key, long incrValue) {
    increment(key.getDeclaringClass().getName(), key.name(), incrValue);
  }

  public void increment(String groupName, String counterName, long value) {
    SparkCounter counter = getGroup(groupName).getCounter(counterName);
    if (counter == null) {
      LOG.error(
        String.format("counter[%s, %s] has not initialized before.", groupName, counterName));
    } else {
      counter.increment(value);
    }
  }

  public long getValue(String groupName, String counterName) {
    SparkCounter counter = getGroup(groupName).getCounter(counterName);
    if (counter == null) {
      LOG.error(
        String.format("counter[%s, %s] has not initialized before.", groupName, counterName));
      return 0;
    } else {
      return counter.getValue();
    }
  }

  public SparkCounter getCounter(String groupName, String counterName) {
    return getGroup(groupName).getCounter(counterName);
  }

  public SparkCounter getCounter(Enum<?> key) {
    return getCounter(key.getDeclaringClass().getName(), key.name());
  }

  private SparkCounterGroup getGroup(String groupName) {
    SparkCounterGroup group = sparkCounterGroups.get(groupName);
    if (group == null) {
      group = new SparkCounterGroup(groupName, groupName, javaSparkContext);
      sparkCounterGroups.put(groupName, group);
    }
    return group;
  }

  public Map<String, SparkCounterGroup> getSparkCounterGroups() {
    return sparkCounterGroups;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    Map<String, SparkCounterGroup> groups = getSparkCounterGroups();
    if (groups != null) {
      for (Map.Entry<String, SparkCounterGroup> groupEntry : groups.entrySet()) {
        String groupName = groupEntry.getKey();
        SparkCounterGroup group = groupEntry.getValue();
        sb.append(groupName).append("\n");
        Map<String, SparkCounter> counters = group.getSparkCounters();
        for (Map.Entry<String, SparkCounter> counterEntry : counters.entrySet()) {
          String counterName = counterEntry.getKey();
          SparkCounter counter = counterEntry.getValue();
          sb.append("\t")
            .append(counterName)
            .append(": ")
            .append(counter.getValue())
            .append("\n");
        }
      }
    }

    return sb.toString();
  }

  /**
   * Create a snapshot of the current counters to send back to the client. This copies the values
   * of all current counters into a new SparkCounters instance that cannot be used to update the
   * counters, but will serialize cleanly when sent back to the RSC client.
   */
  public SparkCounters snapshot() {
    SparkCounters snapshot = new SparkCounters();
    for (SparkCounterGroup group : sparkCounterGroups.values()) {
      SparkCounterGroup groupSnapshot = group.snapshot();
      snapshot.sparkCounterGroups.put(groupSnapshot.getGroupName(), groupSnapshot);
    }
    return snapshot;
  }

}
