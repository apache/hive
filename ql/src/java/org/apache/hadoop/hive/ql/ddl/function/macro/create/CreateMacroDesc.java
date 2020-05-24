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

package org.apache.hadoop.hive.ql.ddl.function.macro.create;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * DDL task description for CREATE TEMPORARY MACRO commands.
 */
@Explain(displayName = "Create Macro", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CreateMacroDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  private final String macroName;
  private final List<String> columnNames;
  private final List<TypeInfo> columnTypes;
  private final ExprNodeDesc body;

  public CreateMacroDesc(String macroName, List<String> columnNames, List<TypeInfo> columnTypes, ExprNodeDesc body) {
    this.macroName = macroName;
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
    this.body = body;
  }

  @Explain(displayName = "name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getName() {
    return macroName;
  }

  @Explain(displayName = "column names", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public List<String> getColumnNames() {
    return columnNames;
  }

  public List<TypeInfo> getColumnTypes() {
    return columnTypes;
  }

  /** For explaining only. */
  @Explain(displayName = "column types", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public List<String> getColumnTypeStrings() {
    return columnTypes.stream()
        .map(typeInfo -> typeInfo.getTypeName())
        .collect(Collectors.toList());
  }

  public ExprNodeDesc getBody() {
    return body;
  }

  /** For explaining only. */
  @Explain(displayName = "body", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getBodyString() {
    return body.toString();
  }
}
