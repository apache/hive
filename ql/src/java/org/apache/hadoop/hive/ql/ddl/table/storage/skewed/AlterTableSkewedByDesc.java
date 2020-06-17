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

package org.apache.hadoop.hive.ql.ddl.table.storage.skewed;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ALTER TABLE ... [SKEWED BY ... ON ...] [[NOT] STORED AS DIRECTORIES] commands.
 */
@Explain(displayName = "Skewed By", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterTableSkewedByDesc extends AbstractAlterTableDesc {
  private static final long serialVersionUID = 1L;

  private final List<String> skewedColumnNames;
  private final List<List<String>> skewedColumnValues;
  private final boolean isStoredAsDirectories;

  public AlterTableSkewedByDesc(TableName tableName, List<String> skewedColumnNames,
      List<List<String>> skewedColumnValues, boolean isStoredAsDirectories) throws SemanticException {
    super(AlterTableType.SKEWED_BY, tableName, null, null, false, false, null);
    this.skewedColumnNames = skewedColumnNames;
    this.skewedColumnValues = skewedColumnValues;
    this.isStoredAsDirectories = isStoredAsDirectories;
  }

  @Explain(displayName = "skewedColumnNames", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public List<String> getSkewedColumnNames() {
    return skewedColumnNames;
  }

  public List<List<String>> getSkewedColumnValues() {
    return skewedColumnValues;
  }

  // for Explain only
  @Explain(displayName = "skewedColumnValues", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public List<String> getSkewedColumnValuesForExplain() {
    return skewedColumnValues.stream().map(l -> l.toString()).collect(Collectors.toList());
  }

  @Explain(displayName = "isStoredAsDirectories", displayOnlyOnTrue = true,
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isStoredAsDirectories() {
    return isStoredAsDirectories;
  }

  @Override
  public boolean mayNeedWriteId() {
    return true;
  }
}
