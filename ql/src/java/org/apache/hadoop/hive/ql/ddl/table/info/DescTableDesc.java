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

package org.apache.hadoop.hive.ql.ddl.table.info;

import java.io.Serializable;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.DDLTask2;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for DESC table_name commands.
 */
@Explain(displayName = "Describe Table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class DescTableDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  private static final String SCHEMA = "col_name,data_type,comment#string:string:string";
  private static final String COL_STATS_SCHEMA = "col_name,data_type,min,max,num_nulls," +
      "distinct_count,avg_col_len,max_col_len,num_trues,num_falses,bitVector,comment" +
      "#string:string:string:string:string:string:string:string:string:string:string:string";
  public static String getSchema(boolean colStats) {
    return colStats ? COL_STATS_SCHEMA : SCHEMA;
  }

  static {
    DDLTask2.registerOperation(DescTableDesc.class, DescTableOperation.class);
  }

  private final String resFile;
  private final String tableName;
  private final Map<String, String> partSpec;
  private final String colPath;
  private final boolean isExt;
  private final boolean isFormatted;

  public DescTableDesc(Path resFile, String tableName, Map<String, String> partSpec, String colPath, boolean isExt,
      boolean isFormatted) {
    this.resFile = resFile.toString();
    this.tableName = tableName;
    this.partSpec = partSpec;
    this.colPath = colPath;
    this.isExt = isExt;
    this.isFormatted = isFormatted;
  }

  @Explain(displayName = "result file", explainLevels = { Level.EXTENDED })
  public String getResFile() {
    return resFile;
  }

  @Explain(displayName = "table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return tableName;
  }

  @Explain(displayName = "partition", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Map<String, String> getPartSpec() {
    return partSpec;
  }

  public String getColumnPath() {
    return colPath;
  }

  public boolean isExt() {
    return isExt;
  }

  public boolean isFormatted() {
    return isFormatted;
  }
}
