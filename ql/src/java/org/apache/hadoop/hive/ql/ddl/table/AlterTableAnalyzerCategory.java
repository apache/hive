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

package org.apache.hadoop.hive.ql.ddl.table;

import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLSemanticAnalyzerCategory;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

/**
 * Alter Table category helper. It derives the actual type of the command from the root element, by selecting the type
 * of the second child, as the Alter Table commands have this structure: tableName command partitionSpec?
 */
@DDLType(types = HiveParser.TOK_ALTERTABLE)
public class AlterTableAnalyzerCategory implements DDLSemanticAnalyzerCategory {
  @Override
  public int getType(ASTNode root) {
    return root.getChild(1).getType();
  }
}
