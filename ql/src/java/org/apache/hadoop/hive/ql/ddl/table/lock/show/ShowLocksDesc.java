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

package org.apache.hadoop.hive.ql.ddl.table.lock.show;

import java.io.Serializable;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for SHOW LOCKS commands.
 */
@Explain(displayName = "Show Locks", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ShowLocksDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  private static final String OLD_FORMAT_SCHEMA = "tab_name,mode#string:string";
  private static final String NEW_FORMAT_SCHEMA = "lockid,database,table,partition,lock_state," +
      "blocked_by,lock_type,transaction_id,last_heartbeat,acquired_at,user,hostname,agent_info#" +
      "string:string:string:string:string:string:string:string:string:string:string:string:string";

  private final String resFile;
  private final String dbName;
  private final String tableName;
  private final Map<String, String> partSpec;
  private final boolean isExt;
  private final boolean isNewFormat;

  public ShowLocksDesc(Path resFile, String dbName, boolean isExt, boolean isNewFormat) {
    this.resFile = resFile.toString();
    this.dbName = dbName;
    this.tableName = null;
    this.partSpec = null;
    this.isExt = isExt;
    this.isNewFormat = isNewFormat;
  }

  public ShowLocksDesc(Path resFile, String tableName, Map<String, String> partSpec, boolean isExt,
      boolean isNewFormat) {
    this.resFile = resFile.toString();
    this.dbName = null;
    this.tableName = tableName;
    this.partSpec = partSpec;
    this.isExt = isExt;
    this.isNewFormat = isNewFormat;
  }

  @Explain(displayName = "result file", explainLevels = { Level.EXTENDED })
  public String getResFile() {
    return resFile;
  }

  @Explain(displayName = "dbName", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getDbName() {
    return dbName;
  }

  @Explain(displayName = "table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return tableName;
  }

  @Explain(displayName = "partition", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Map<String, String> getPartSpec() {
    return partSpec;
  }

  public boolean isExt() {
    return isExt;
  }

  public boolean isNewFormat() {
    return isNewFormat;
  }

  public String getSchema() {
    return isNewFormat ? NEW_FORMAT_SCHEMA : OLD_FORMAT_SCHEMA;
  }
}
