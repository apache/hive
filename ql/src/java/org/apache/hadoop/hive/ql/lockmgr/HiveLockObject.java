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
  
  String [] pathNames = null;

  /* user supplied data for that object */
  private String    data;

  public HiveLockObject() {
    this.data = null;
  }
  
  public HiveLockObject(String[] paths, String lockData) {
    this.pathNames = paths;
    this.data = lockData;
  }
  
  public HiveLockObject(Table tbl, String lockData) {
    this(new String[] {tbl.getDbName(), tbl.getTableName()}, lockData);
  }

  public HiveLockObject(Partition par, String lockData) {
    this(new String[] { par.getTable().getDbName(),
        par.getTable().getTableName(), par.getName() }, lockData);
  }

  public String getName() {
    if (this.pathNames == null) {
      return null;
    }
    String ret = "";
    boolean first = true;
    for (int i = 0; i < pathNames.length; i++) {
      if (!first) {
        ret = ret + "@";
      } else {
        first = false;
      }
      ret = ret + pathNames[i];
    }
    return ret;
  }

  public String getData() {
    return data;
  }

  public void setData(String data) {
    this.data = data;
  }

}
