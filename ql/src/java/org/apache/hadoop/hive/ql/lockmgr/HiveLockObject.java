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

package org.apache.hadoop.hive.ql.lockmgr;

import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

public class HiveLockObject {
  /**
   * The table.
   */
  private Table t;

  /**
   * The partition. This is null for a non partitioned table.
   */
  private Partition p;

  /* user supplied data for that object */
  private String    data;

  public HiveLockObject() {
    this.t = null;
    this.p = null;
    this.data = null;
  }

  public HiveLockObject(Table t, String data) {
    this.t = t;
    this.p = null;
    this.data = data;
  }

  public HiveLockObject(Partition p, String data) {
    this.t = null;
    this.p = p;
    this.data = data;
  }

  public Table getTable() {
    return t;
  }

  public void setTable (Table t) {
    this.t = t;
  }

  public Partition getPartition() {
    return p;
  }

  public void setPartition (Partition p) {
    this.p = p;
  }

  public String getName() {
    if (t != null) {
      return t.getCompleteName();
    }
    else {
      return p.getCompleteName();
    }
  }

  public String getData() {
    return data;
  }

  public void setData(String data) {
    this.data = data;
  }

}
