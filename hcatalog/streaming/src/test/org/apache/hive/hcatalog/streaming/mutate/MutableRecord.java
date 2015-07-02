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

package org.apache.hive.hcatalog.streaming.mutate;

import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.io.Text;

public class MutableRecord {

  // Column 0
  public final int id;
  // Column 1
  public final Text msg;
  // Column 2
  public RecordIdentifier rowId;

  public MutableRecord(int id, String msg, RecordIdentifier rowId) {
    this.id = id;
    this.msg = new Text(msg);
    this.rowId = rowId;
  }

  public MutableRecord(int id, String msg) {
    this.id = id;
    this.msg = new Text(msg);
    rowId = null;
  }

  @Override
  public String toString() {
    return "MutableRecord [id=" + id + ", msg=" + msg + ", rowId=" + rowId + "]";
  }

}
