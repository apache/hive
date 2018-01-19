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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;

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
  private transient CreateTableDesc ctasCreateTableDesc;
  private transient CreateViewDesc createViewDesc;
  private boolean isMmCtas;

  public LoadFileDesc(final LoadFileDesc o) {
    super(o.getSourcePath(), o.getWriteType());

    this.targetDir = o.targetDir;
    this.isDfsDir = o.isDfsDir;
    this.columns = o.columns;
    this.columnTypes = o.columnTypes;
    this.isMmCtas = o.isMmCtas;
    this.ctasCreateTableDesc = o.ctasCreateTableDesc;
    this.createViewDesc = o.createViewDesc;
  }

  public LoadFileDesc(final CreateTableDesc createTableDesc, final CreateViewDesc createViewDesc,
      final Path sourcePath, final Path targetDir, final boolean isDfsDir,
      final String columns, final String columnTypes, AcidUtils.Operation writeType, boolean isMmCtas) {
    this(sourcePath, targetDir, isDfsDir, columns, columnTypes, writeType, isMmCtas);
    if (createTableDesc != null && createTableDesc.isCTAS()) {
      this.ctasCreateTableDesc = createTableDesc;
    }
    if (createViewDesc != null && createViewDesc.isMaterialized()) {
      this.createViewDesc = createViewDesc;
    }
  }

  public LoadFileDesc(final Path sourcePath, final Path targetDir,
      final boolean isDfsDir, final String columns, final String columnTypes, boolean isMmCtas) {
    this(sourcePath, targetDir, isDfsDir, columns, columnTypes, AcidUtils.Operation.NOT_ACID, isMmCtas);
  }

  private LoadFileDesc(final Path sourcePath, final Path targetDir,
      final boolean isDfsDir, final String columns,
      final String columnTypes, AcidUtils.Operation writeType, boolean isMmCtas) {
    super(sourcePath, writeType);
    if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
      Utilities.FILE_OP_LOGGER.trace("creating LFD from " + sourcePath + " to " + targetDir);
    }
    this.targetDir = targetDir;
    this.isDfsDir = isDfsDir;
    this.columns = columns;
    this.columnTypes = columnTypes;
    this.isMmCtas = isMmCtas;
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

  public CreateTableDesc getCtasCreateTableDesc() {
    return ctasCreateTableDesc;
  }

  public CreateViewDesc getCreateViewDesc() {
    return createViewDesc;
  }

  public boolean isMmCtas() {
    return isMmCtas;
  }
}
