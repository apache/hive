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

package org.apache.hadoop.hive.ql.ddl.table.create.show;


import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.exec.DDLPlanUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * Operation process showing the creation of a table.
 */
public class ShowCreateTableOperation extends DDLOperation<ShowCreateTableDesc> {

  public ShowCreateTableOperation(DDLOperationContext context, ShowCreateTableDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    // get the create table statement for the table and populate the output
    try (DataOutputStream outStream = ShowUtils.getOutputStream(new Path(desc.getResFile()), context)) {
      Table table = context.getDb().getTable(desc.getDatabaseName(), desc.getTableName());
      DDLPlanUtils ddlObj = new DDLPlanUtils();
      String command;
      if (table.isView()) {
        command = escapeSqlTabs(ddlObj.getCreateViewCommand(table, desc.isRelative()));
      } else {
        List<String> commands = new ArrayList<>();
        commands.add(ddlObj.getCreateTableCommand(table, desc.isRelative()));
        String primaryKeyStmt = ddlObj.getAlterTableStmtPrimaryKeyConstraint(table.getPrimaryKeyInfo());
        if (primaryKeyStmt != null) {
          commands.add(primaryKeyStmt);
        }
        commands.addAll(ddlObj.populateConstraints(table));
        command = String.join("\n", commands);
      }
      outStream.write(command.getBytes(StandardCharsets.UTF_8));
      return 0;
    } catch (IOException e) {
      LOG.info("Show create table failed", e);
      return 1;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  private static String escapeSqlTabs(String sql) {
    if (sql == null || sql.indexOf('\t') < 0) {
      return sql;
    }
    StringBuilder result = new StringBuilder(sql.length());
    char quote = 0;
    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      if (quote == 0) { // outside string literal
        if (c == '\'' || c == '"') {
          quote = c;
          result.append(c);
        } else if (c == '\t') {
          result.append(' ');
        } else {
          result.append(c);
        }
      } else { // inside string literal
        if (c == '\\' && i + 1 < sql.length()) {
          result.append(c);
          result.append(sql.charAt(i + 1));
          i++;
        } else if (c == quote) {
          quote = 0;
          result.append(c);
        } else if (c == '\t') {
          result.append("\\t");
        } else {
          result.append(c);
        }
      }
    }
    return result.toString();
  }
}
