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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.PTFUtils;

/**
 * LoadFileDesc.
 *
 */
public class LoadFileDesc extends LoadDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  private transient Path targetDir;
  private boolean isDfsDir;
  // list of columns, comma separated
  private String columns;
  private String columnTypes;
  private String destinationCreateTable;

  static {
	  PTFUtils.makeTransient(LoadFileDesc.class, "targetDir");
  }
  public LoadFileDesc() {
  }

  public LoadFileDesc(final CreateTableDesc createTableDesc, final Path sourcePath,
      final Path targetDir,
      final boolean isDfsDir, final String columns, final String columnTypes) {
    this(sourcePath, targetDir, isDfsDir, columns, columnTypes);
    if (createTableDesc != null && createTableDesc.getDatabaseName() != null
        && createTableDesc.getTableName() != null) {
      destinationCreateTable = (createTableDesc.getTableName().contains(".") ? "" : createTableDesc
          .getDatabaseName() + ".")
          + createTableDesc.getTableName();
    }
  }

  public LoadFileDesc(final Path sourcePath, final Path targetDir,
      final boolean isDfsDir, final String columns, final String columnTypes) {

    super(sourcePath);
    this.targetDir = targetDir;
    this.isDfsDir = isDfsDir;
    this.columns = columns;
    this.columnTypes = columnTypes;
  }

  @Explain(displayName = "destination")
  public Path getTargetDir() {
    return targetDir;
  }

  public void setTargetDir(final Path targetDir) {
    this.targetDir = targetDir;
  }

  @Explain(displayName = "hdfs directory")
  public boolean getIsDfsDir() {
    return isDfsDir;
  }

  public void setIsDfsDir(final boolean isDfsDir) {
    this.isDfsDir = isDfsDir;
  }

  /**
   * @return the columns
   */
  public String getColumns() {
    return columns;
  }

  /**
   * @param columns
   *          the columns to set
   */
  public void setColumns(String columns) {
    this.columns = columns;
  }

  /**
   * @return the columnTypes
   */
  public String getColumnTypes() {
    return columnTypes;
  }

  /**
   * @param columnTypes
   *          the columnTypes to set
   */
  public void setColumnTypes(String columnTypes) {
    this.columnTypes = columnTypes;
  }

  /**
   * @return the destinationCreateTable
   */
  public String getDestinationCreateTable(){
    return destinationCreateTable;
  }
}
