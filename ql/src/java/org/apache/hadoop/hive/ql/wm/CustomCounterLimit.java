/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.wm;

/**
 * Custom counters with limits (this will only work if the execution engine exposes this counter)
 */
public class CustomCounterLimit implements CounterLimit {

  private String counterName;
  private long limit;

  CustomCounterLimit(final String counterName, final long limit) {
    this.counterName = counterName;
    this.limit = limit;
  }

  @Override
  public String getName() {
    return counterName;
  }

  @Override
  public long getLimit() {
    return limit;
  }

  @Override
  public CounterLimit clone() {
    return new CustomCounterLimit(counterName, limit);
  }

  @Override
  public String toString() {
    return "counter: " + counterName + " limit: " + limit;
  }

  @Override
  public int hashCode() {
    int hash = 31 * counterName.hashCode();
    hash += 31 * limit;
    return 31 * hash;
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof CustomCounterLimit)) {
      return false;
    }

    if (other == this) {
      return true;
    }

    CustomCounterLimit otherVcl = (CustomCounterLimit) other;
    return counterName.equalsIgnoreCase(otherVcl.counterName) && limit == otherVcl.limit;
  }
}
