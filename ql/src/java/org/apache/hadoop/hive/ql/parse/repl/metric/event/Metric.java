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
package org.apache.hadoop.hive.ql.parse.repl.metric.event;

/**
 * Class for defining the unit metric.
 */
public class Metric {
  private String name;
  private long currentCount;
  private long totalCount;

  public Metric() {

  }

  public Metric(String name, long totalCount) {
    this.name = name;
    this.totalCount = totalCount;
  }

  public Metric(Metric metric) {
    this.name = metric.name;
    this.totalCount = metric.totalCount;
    this.currentCount = metric.currentCount;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getCurrentCount() {
    return currentCount;
  }

  public void setCurrentCount(long currentCount) {
    this.currentCount = currentCount;
  }

  public long getTotalCount() {
    return totalCount;
  }

  public void setTotalCount(long totalCount) {
    this.totalCount = totalCount;
  }
}
