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
        command = ddlObj.getCreateViewCommand(table, desc.isRelative());
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
}
