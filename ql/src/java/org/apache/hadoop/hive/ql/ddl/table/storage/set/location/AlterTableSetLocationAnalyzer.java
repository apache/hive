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

package org.apache.hadoop.hive.ql.ddl.table.storage.set.location;

import java.io.FileNotFoundException;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for set location commands.
 */
@DDLType(types = {HiveParser.TOK_ALTERTABLE_LOCATION, HiveParser.TOK_ALTERPARTITION_LOCATION})
public class AlterTableSetLocationAnalyzer extends AbstractAlterTableAnalyzer {
  public AlterTableSetLocationAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {
    String newLocation = unescapeSQLString(command.getChild(0).getText());
    try {
      // To make sure host/port pair is valid, the status of the location does not matter
      FileSystem.get(new URI(newLocation), conf).getFileStatus(new Path(newLocation));
    } catch (FileNotFoundException e) {
      // Only check host/port pair is valid, whether the file exist or not does not matter
    } catch (Exception e) {
      throw new SemanticException("Can not establish connection to server for " + newLocation , e);
    }

    outputs.add(toWriteEntity(newLocation));
    AlterTableSetLocationDesc desc = new AlterTableSetLocationDesc(tableName, partitionSpec, newLocation);
    Table table = getTable(tableName);
    if (AcidUtils.isTransactionalTable(table)) {
      setAcidDdlDesc(desc);
    }

    addInputsOutputsAlterTable(tableName, partitionSpec, desc, AlterTableType.ALTERLOCATION, false);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }
}
