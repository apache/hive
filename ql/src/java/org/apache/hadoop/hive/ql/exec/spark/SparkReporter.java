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
package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hive.spark.counter.SparkCounters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;

import java.io.Serializable;

/**
 * Implement SparkReporter for Hive operator level statistics collection, and throw
 * UnsupportedOperationException for other unrelated methods, so if any Hive feature
 * depends on these unimplemented methods, we could go back here quickly and enable it.
 */
public class SparkReporter implements Reporter, Serializable {

  private SparkCounters sparkCounters;
  private String status;
  public SparkReporter(SparkCounters sparkCounters) {
    this.sparkCounters = sparkCounters;
  }

  @Override
  public void setStatus(String status) {
    this.status = status;
  }

  public String getStatus() {
    return this.status;
  }

  @Override
  public Counter getCounter(Enum<?> name) {
    throw new UnsupportedOperationException("do not support this method now.");
  }

  @Override
  public Counter getCounter(String group, String name) {
    throw new UnsupportedOperationException("do not support this method now.");
  }

  @Override
  public void incrCounter(Enum<?> key, long amount) {
    sparkCounters.increment(key.getDeclaringClass().getName(), key.name(), amount);
  }

  @Override
  public void incrCounter(String group, String counter, long amount) {
    sparkCounters.increment(group, counter, amount);
  }

  @Override
  public InputSplit getInputSplit() {
    throw new UnsupportedOperationException("do not support this method now.");
  }

  @Override
  public float getProgress() {
    throw new UnsupportedOperationException("do not support this method now.");
  }

  @Override
  public void progress() {
    //do not support task level progress, do nothing here.
  }
}
