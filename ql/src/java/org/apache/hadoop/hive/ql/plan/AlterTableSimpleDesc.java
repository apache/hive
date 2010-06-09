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

package org.apache.hadoop.hive.ql.plan;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.plan.AlterTableDesc.AlterTableTypes;

/**
 * Contains information needed to modify a partition or a table
 */
public class AlterTableSimpleDesc extends DDLDesc {
  private String tableName;
  private String dbName;
  private LinkedHashMap<String, String> partSpec;

  AlterTableTypes type;

  public AlterTableSimpleDesc() {
  }

  /**
   * @param dbName
   *          database that contains the table / partition
   * @param tableName
   *          table containing the partition
   * @param partSpec
   *          partition specification. Null if touching a table.
   */
  public AlterTableSimpleDesc(String dbName, String tableName,
      Map<String, String> partSpec, AlterTableDesc.AlterTableTypes type) {
    super();
    this.dbName = dbName;
    this.tableName = tableName;
    if(partSpec == null) {
      this.partSpec = null;
    } else {
      this.partSpec = new LinkedHashMap<String,String>(partSpec);
    }
    this.type = type;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public AlterTableDesc.AlterTableTypes getType() {
    return type;
  }

  public void setType(AlterTableDesc.AlterTableTypes type) {
    this.type = type;
  }

  public LinkedHashMap<String, String> getPartSpec() {
    return partSpec;
  }

  public void setPartSpec(LinkedHashMap<String, String> partSpec) {
    this.partSpec = partSpec;
  }

}
