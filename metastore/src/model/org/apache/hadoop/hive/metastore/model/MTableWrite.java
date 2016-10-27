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

package org.apache.hadoop.hive.metastore.model;

public class MTableWrite {
  private MTable table;
  private long writeId;
  private String state;
  private long lastHeartbeat;
  private long created;

  public MTableWrite() {}

  public MTableWrite(MTable table, long writeId, String state, long lastHeartbeat, long created) {
    this.table = table;
    this.writeId = writeId;
    this.state = state;
    this.lastHeartbeat = lastHeartbeat;
    this.created = created;
  }

  public MTable getTable() {
    return table;
  }

  public long getWriteId() {
    return writeId;
  }

  public String getState() {
    return state;
  }

  public long getLastHeartbeat() {
    return lastHeartbeat;
  }

  public long getCreated() {
    return created;
  }

  public void setTable(MTable table) {
    this.table = table;
  }

  public void setWriteId(long writeId) {
    this.writeId = writeId;
  }

  public void setState(String state) {
    this.state = state;
  }

  public void setLastHeartbeat(long lastHeartbeat) {
    this.lastHeartbeat = lastHeartbeat;
  }

  public void setCreated(long created) {
    this.created = created;
  }
}
