/**
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
 * Vertex specific counters with limits
 */
public class VertexCounterLimit implements CounterLimit {
  public enum VertexCounter {
    TOTAL_TASKS
  }

  private VertexCounter vertexCounter;
  private long limit;

  public VertexCounterLimit(final VertexCounter vertexCounter, final long limit) {
    this.vertexCounter = vertexCounter;
    this.limit = limit;
  }

  @Override
  public String getName() {
    return vertexCounter.name();
  }

  @Override
  public long getLimit() {
    return limit;
  }

  @Override
  public CounterLimit clone() {
    return new VertexCounterLimit(vertexCounter, limit);
  }

  @Override
  public String toString() {
    return "counter: " + vertexCounter.name() + " limit: " + limit;
  }

  @Override
  public int hashCode() {
    int hash = 31 * vertexCounter.hashCode();
    hash += 31 * limit;
    return 31 * hash;
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof VertexCounterLimit)) {
      return false;
    }

    if (other == this) {
      return true;
    }

    VertexCounterLimit otherVcl = (VertexCounterLimit) other;
    return vertexCounter.equals(otherVcl.vertexCounter) && limit == otherVcl.limit;
  }
}
