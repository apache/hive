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

package org.apache.hadoop.hive.ql.ddl.database.alter.location;

import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.database.alter.AbstractAlterDatabaseAnalyzer;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for database set managed location commands.
 */
@DDLType(types = HiveParser.TOK_ALTERDATABASE_MANAGEDLOCATION)
public class AlterDatabaseSetManagedLocationAnalyzer extends AbstractAlterDatabaseAnalyzer {
  public AlterDatabaseSetManagedLocationAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    String databaseName = getUnescapedName((ASTNode) root.getChild(0));
    String newLocation = unescapeSQLString(root.getChild(1).getText());

    outputs.add(toWriteEntity(newLocation));

    AlterDatabaseSetManagedLocationDesc desc = new AlterDatabaseSetManagedLocationDesc(databaseName, newLocation);
    addAlterDatabaseDesc(desc);
  }
}
