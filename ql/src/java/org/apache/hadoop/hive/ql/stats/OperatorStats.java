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
package org.apache.hadoop.hive.ql.stats;

import com.google.common.base.Objects;

/**
 * Holds information an operator's statistics.
 */
public final class OperatorStats {

  /** Marker class to help with plan elements which will collect invalid statistics */
  public static class IncorrectRuntimeStatsMarker {
  }

  /** Marker class to help with plan elements which will collect invalid statistics */
  public static class MayNotUseForRelNodes {
  }

  private String operatorId;
  private long outputRecords;

  // for jackson
  @SuppressWarnings("unused")
  private OperatorStats() {
  }

  public OperatorStats(final String opId) {
    this.operatorId = opId;
    this.outputRecords = -1;
  }

  public long getOutputRecords() {
    return outputRecords;
  }

  public void setOutputRecords(final long outputRecords) {
    this.outputRecords = outputRecords;
  }

  public String getOperatorId() {
    return operatorId;
  }

  @Override
  public String toString() {
    return String.format("OperatorStats %s records: %d", operatorId, outputRecords);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(operatorId, outputRecords);
  }

  @Override
  public boolean equals(Object obj) {
    if(obj==null || obj.getClass()!= OperatorStats.class){
      return false;
    }
    OperatorStats o = (OperatorStats) obj;
    return Objects.equal(operatorId, o.operatorId) &&
        Objects.equal(outputRecords, o.outputRecords);
  }


}
