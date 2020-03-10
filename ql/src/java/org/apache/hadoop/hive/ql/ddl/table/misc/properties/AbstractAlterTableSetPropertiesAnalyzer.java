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

package org.apache.hadoop.hive.ql.ddl.table.misc.properties;

import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableDesc;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for setting the properties of table like entities commands.
 */
public abstract class AbstractAlterTableSetPropertiesAnalyzer extends AbstractAlterTablePropertiesAnalyzer {
  public AbstractAlterTableSetPropertiesAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  protected AbstractAlterTableDesc createDesc(ASTNode command, TableName tableName, Map<String, String> partitionSpec,
      Map<String, String> properties, boolean isToTxn, boolean isExplicitStatsUpdate,
      EnvironmentContext environmentContext) throws SemanticException {
    addPropertyReadEntry(properties, inputs);
    boolean isAcidConversion = isToTxn && AcidUtils.isFullAcidTable(properties)
        && !AcidUtils.isFullAcidTable(getTable(tableName, true));
    return new AlterTableSetPropertiesDesc(tableName, partitionSpec, null, isView(), properties,
        isExplicitStatsUpdate, isAcidConversion, environmentContext);
  }

  protected abstract boolean isView();
}
