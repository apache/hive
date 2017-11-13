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
 * File system specific counters with defined limits
 */
public class FileSystemCounterLimit implements CounterLimit {

  public enum FSCounter {
    BYTES_READ,
    BYTES_WRITTEN,
    SHUFFLE_BYTES
  }

  private String scheme;
  private FSCounter fsCounter;
  private long limit;

  FileSystemCounterLimit(final String scheme, final FSCounter fsCounter, final long limit) {
    this.scheme = scheme == null || scheme.isEmpty() ? "" : scheme.toUpperCase();
    this.fsCounter = fsCounter;
    this.limit = limit;
  }

  static FileSystemCounterLimit fromName(final String counterName, final long limit) {
    String counterNameStr = counterName.toUpperCase();
    for (FSCounter fsCounter : FSCounter.values()) {
      if (counterNameStr.endsWith(fsCounter.name())) {
        int startIdx = counterNameStr.indexOf(fsCounter.name());
        if (startIdx == 0) { // exact match
          return new FileSystemCounterLimit(null, FSCounter.valueOf(counterName), limit);
        } else {
          String scheme = counterNameStr.substring(0, startIdx - 1);
          // schema/counter name validation will be done in grammar as part of HIVE-17622
          return new FileSystemCounterLimit(scheme, FSCounter.valueOf(fsCounter.name()), limit);
        }
      }
    }

    throw new IllegalArgumentException("Invalid counter name specified " + counterName.toUpperCase() + "");
  }

  @Override
  public String getName() {
    return scheme.isEmpty() ? fsCounter.name() : scheme.toUpperCase() + "_" + fsCounter.name();
  }

  @Override
  public long getLimit() {
    return limit;
  }

  @Override
  public CounterLimit clone() {
    return new FileSystemCounterLimit(scheme, fsCounter, limit);
  }

  @Override
  public String toString() {
    return "counter: " + getName() + " limit: " + limit;
  }

  @Override
  public int hashCode() {
    int hash = 31 * scheme.hashCode();
    hash += 31 * fsCounter.hashCode();
    hash += 31 * limit;
    return 31 * hash;
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof FileSystemCounterLimit)) {
      return false;
    }

    if (other == this) {
      return true;
    }

    FileSystemCounterLimit otherFscl = (FileSystemCounterLimit) other;
    return scheme.equals(otherFscl.scheme) && fsCounter.equals(otherFscl.fsCounter) && limit == otherFscl.limit;
  }
}
