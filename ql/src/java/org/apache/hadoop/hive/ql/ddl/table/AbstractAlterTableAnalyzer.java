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

import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLDesc.DDLDescWithWriteId;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Abstract ancestor of all Alter Table analyzer, that have this structure:
 * tableName command partitionSpec?
 */
public abstract class AbstractAlterTableAnalyzer extends AbstractBaseAlterTableAnalyzer {

  public AbstractAlterTableAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    TableName tableName = getQualifiedTableName((ASTNode) root.getChild(0), MetaStoreUtils.getDefaultCatalog(conf));

    ASTNode command = (ASTNode)root.getChild(1);

    Map<String, String> partitionSpec = null;
    ASTNode partitionSpecNode = (ASTNode)root.getChild(2);
    if (partitionSpecNode != null) {
      //  We can use alter table partition rename to convert/normalize the legacy partition
      //  column values. In so, we should not enable the validation to the old partition spec
      //  passed in this command.
      if (command.getType() == HiveParser.TOK_ALTERTABLE_RENAMEPART) {
        partitionSpec = getPartSpec(partitionSpecNode);
      } else {
        partitionSpec = getValidatedPartSpec(getTable(tableName), partitionSpecNode, conf, false);
      }
    }

    analyzeCommand(tableName, partitionSpec, command);
  }

  protected abstract void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException;

  protected void setAcidDdlDesc(Table table, DDLDescWithWriteId desc) {

    if (AcidUtils.isTransactionalTable(table)) {
      setAcidDdlDesc(desc);
    }
  }
}
