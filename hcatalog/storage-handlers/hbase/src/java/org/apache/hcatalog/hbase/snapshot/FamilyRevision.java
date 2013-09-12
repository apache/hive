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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.hbase.snapshot;


/**
 * A FamiliyRevision class consists of a revision number and a expiration
 * timestamp. When a write transaction starts, the transaction
 * object is appended to the transaction list of the each column
 * family and stored in the corresponding znode. When a write transaction is
 * committed, the transaction object is removed from the list.
 */
public class FamilyRevision implements
    Comparable<FamilyRevision> {

  private long revision;

  private long timestamp;

  /**
   * Create a FamilyRevision object
   * @param rev revision number
   * @param ts expiration timestamp
   */
  FamilyRevision(long rev, long ts) {
    this.revision = rev;
    this.timestamp = ts;
  }

  public long getRevision() {
    return revision;
  }

  public long getExpireTimestamp() {
    return timestamp;
  }

  void setExpireTimestamp(long ts) {
    timestamp = ts;
  }

  @Override
  public String toString() {
    String description = "revision: " + revision + " ts: " + timestamp;
    return description;
  }

  @Override
  public int compareTo(FamilyRevision o) {
    long d = revision - o.getRevision();
    return (d < 0) ? -1 : (d > 0) ? 1 : 0;
  }


}
