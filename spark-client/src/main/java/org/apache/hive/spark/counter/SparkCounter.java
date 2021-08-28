/*
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

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;

public class SparkCounter implements Serializable {

  private String name;
  private String displayName;
  private AccumulatorV2<Long, Long> accumulator;

  // Values of accumulators can only be read on the SparkContext side. This field is used when
  // creating a snapshot to be sent to the RSC client.
  private long accumValue;

  public SparkCounter() {
    // For serialization.
  }

  private SparkCounter(
      String name,
      String displayName,
      long value) {
    this.name = name;
    this.displayName = displayName;
    this.accumValue = value;
  }

  public SparkCounter(
    String name,
    String displayName,
    String groupName,
    long initValue,
    JavaSparkContext sparkContext) {

    String accumulatorName = groupName + "_" + name;
    LongAccumulator longAccumulator = sparkContext.sc().longAccumulator(accumulatorName);

    longAccumulator.setValue(initValue);

    this.name = name;
    this.displayName = displayName;
    this.accumulator = longAccumulator;
  }

  public long getValue() {
    if (accumulator != null) {
      return accumulator.value();
    } else {
      return accumValue;
    }
  }

  public void increment(long incr) {
    accumulator.add(incr);
  }

  public String getName() {
    return name;
  }

  public String getDisplayName() {
    return displayName;
  }

  public void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  SparkCounter snapshot() {
    return new SparkCounter(name, displayName, accumulator.value());
  }
}
