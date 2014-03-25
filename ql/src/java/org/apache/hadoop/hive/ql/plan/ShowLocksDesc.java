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

import java.io.Serializable;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

/**
 * ShowLocksDesc.
 *
 */
@Explain(displayName = "Show Locks")
public class ShowLocksDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  String resFile;
  String dbName;
  String tableName;
  HashMap<String, String> partSpec;
  boolean isExt;
  boolean isNewLockFormat;

  /**
   * table name for the result of show locks.
   */
  private static final String table = "showlocks";
  /**
   * thrift ddl for the result of show locks.
   */
  private static final String schema = "tab_name,mode#string:string";

  /**
   * Schema for use with db txn manager.
   */
  private static final String newFormatSchema = "lockid,database,table,partition,lock_state," +
      "lock_type,transaction_id,last_heartbeat,acquired_at,user," +
      "hostname#string:string:string:string:string:string:string:string:string:string:string";

  public String getDatabase() {
    return dbName;
  }

  public String getTable() {
    return table;
  }

  public String getSchema() {
    if (isNewLockFormat) return newFormatSchema;
    else return schema;
  }

  public ShowLocksDesc() {
  }

  /**
   * @param resFile
   */
  public ShowLocksDesc(Path resFile, String dbName, boolean isExt, boolean isNewFormat) {
    this.resFile   = resFile.toString();
    this.partSpec  = null;
    this.tableName = null;
    this.isExt     = isExt;
    this.dbName = dbName;
    isNewLockFormat = isNewFormat;
  }

  /**
   * @param resFile
   */
  public ShowLocksDesc(Path resFile, String tableName,
                       HashMap<String, String> partSpec, boolean isExt, boolean isNewFormat) {
    this.resFile   = resFile.toString();
    this.partSpec  = partSpec;
    this.tableName = tableName;
    this.isExt     = isExt;
    isNewLockFormat = isNewFormat;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  /**
   * @return the tableName
   */
  @Explain(displayName = "table")
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName
   *          the tableName to set
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * @return the partSpec
   */
  @Explain(displayName = "partition")
  public HashMap<String, String> getPartSpec() {
    return partSpec;
  }

  /**
   * @param partSpec
   *          the partSpec to set
   */
  public void setPartSpecs(HashMap<String, String> partSpec) {
    this.partSpec = partSpec;
  }

  /**
   * @return the resFile
   */
  @Explain(displayName = "result file", normalExplain = false)
  public String getResFile() {
    return resFile;
  }

  /**
   * @param resFile
   *          the resFile to set
   */
  public void setResFile(String resFile) {
    this.resFile = resFile;
  }

  /**
   * @return the isExt
   */
  public boolean isExt() {
    return isExt;
  }

  /**
   * @param isExt
   *          the isExt to set
   */
  public void setExt(boolean isExt) {
    this.isExt = isExt;
  }

  public boolean isNewFormat() {
    return isNewLockFormat;
  }
}
