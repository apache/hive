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

package org.apache.hadoop.hive.ql.ddl.table.info.show.properties;

import java.io.Serializable;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for SHOW TABLE PROPERTIES commands.
 */
@Explain(displayName = "Show Table Properties", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ShowTablePropertiesDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  public static final String SCHEMA = "prpt_name,prpt_value#string:string";

  private final String resFile;
  private final TableName tableName;
  private final String propertyName;

  public ShowTablePropertiesDesc(String resFile, TableName tableName, String propertyName) {
    this.resFile = resFile;
    this.tableName = tableName;
    this.propertyName = propertyName;
  }

  public String getResFile() {
    return resFile;
  }

  @Explain(displayName = "result file", explainLevels = { Level.EXTENDED })
  public String getResFileString() {
    return getResFile();
  }

  @Explain(displayName = "table name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return tableName.getNotEmptyDbTable();
  }

  @Explain(displayName = "property name")
  public String getPropertyName() {
    return propertyName;
  }
}
