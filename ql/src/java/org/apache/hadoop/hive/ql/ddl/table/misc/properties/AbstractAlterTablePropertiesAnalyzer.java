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
import java.util.Map.Entry;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableDesc;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.DefaultConstraint;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.NotNullConstraint;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;

/**
 * Analyzer for setting/unsetting the properties of table like entities commands.
 */
@DDLType(types = HiveParser.TOK_ALTERTABLE_PROPERTIES)
public abstract class AbstractAlterTablePropertiesAnalyzer extends AbstractAlterTableAnalyzer {
  public AbstractAlterTablePropertiesAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {
    Map<String, String> properties = getProps((ASTNode) (command.getChild(0)).getChild(0));

    boolean updateStats = validate(tableName, properties);

    EnvironmentContext environmentContext = null;
    if (updateStats) {
      environmentContext = new EnvironmentContext();
      environmentContext.putToProperties(StatsSetupConst.STATS_GENERATED, StatsSetupConst.USER);
    }

    boolean isToTxn = AcidUtils.isTablePropertyTransactional(properties) ||
          properties.containsKey(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES);
    boolean isExplicitStatsUpdate = updateStats && AcidUtils.isTransactionalTable(getTable(tableName, true));

    AbstractAlterTableDesc desc = createDesc(command, tableName, partitionSpec, properties, isToTxn,
        isExplicitStatsUpdate, environmentContext);

    addInputsOutputsAlterTable(tableName, partitionSpec, desc, desc.getType(), isToTxn);
    DDLWork ddlWork = new DDLWork(getInputs(), getOutputs(), desc);
    if (isToTxn) {
      ddlWork.setNeedLock(true); // Hmm... why don't many other operations here need locks?
    }
    if (isToTxn || isExplicitStatsUpdate) {
      setAcidDdlDesc(desc);
    }

    rootTasks.add(TaskFactory.get(ddlWork));
  }

  /**
   * @return If it is executed after an update statistics command.
   */
  private boolean validate(TableName tableName, Map<String, String> properties) throws SemanticException {
    // We need to check if the properties are valid, especially for stats.
    // They might be changed via alter table .. update statistics or alter table .. set tblproperties.
    // If the property is not row_count or raw_data_size, it could not be changed through update statistics.
    boolean changeStats = false;
    for (Entry<String, String> entry : properties.entrySet()) {
      // we make sure that we do not change anything if there is anything wrong.
      if (entry.getKey().equals(StatsSetupConst.ROW_COUNT) || entry.getKey().equals(StatsSetupConst.RAW_DATA_SIZE)) {
        try {
          Long.parseLong(entry.getValue());
          changeStats = true;
        } catch (Exception e) {
          throw new SemanticException("AlterTable " + entry.getKey() + " failed with value " + entry.getValue());
        }
      } else if (entry.getKey().equals("external") && entry.getValue().equals("true")) {
        // if table is being modified to be external we need to make sure existing table
        // doesn't have enabled constraint since constraints are disallowed with such tables
        if (hasConstraintsEnabled(tableName.getTable())) {
          throw new SemanticException(ErrorMsg.INVALID_CSTR_SYNTAX.getMsg(String.format(
              "Table: %s has constraints enabled. Please remove those constraints to change this property.",
              tableName.getNotEmptyDbTable())));
        }
      } else {
        if (queryState.getCommandType().equals(HiveOperation.ALTERTABLE_UPDATETABLESTATS.getOperationName()) ||
            queryState.getCommandType().equals(HiveOperation.ALTERTABLE_UPDATEPARTSTATS.getOperationName())) {
          throw new SemanticException(String.format(
              "AlterTable UpdateStats %s failed because the only valid keys are %s and %s",
              entry.getKey(), StatsSetupConst.ROW_COUNT, StatsSetupConst.RAW_DATA_SIZE));
        }
      }
    }
    return changeStats;
  }

  private boolean hasConstraintsEnabled(String tableName) throws SemanticException{
    NotNullConstraint notNullConstraint = null;
    DefaultConstraint defaultConstraint = null;
    try {
      // retrieve enabled NOT NULL constraint from metastore
      notNullConstraint = Hive.get().getEnabledNotNullConstraints(db.getDatabaseCurrent().getName(), tableName);
      defaultConstraint = Hive.get().getEnabledDefaultConstraints(db.getDatabaseCurrent().getName(), tableName);
    } catch (Exception e) {
      if (e instanceof SemanticException) {
        throw (SemanticException) e;
      } else {
        throw (new RuntimeException(e));
      }
    }
    return
        (notNullConstraint != null && !notNullConstraint.getNotNullConstraints().isEmpty()) ||
        (defaultConstraint != null && !defaultConstraint.getDefaultConstraints().isEmpty());
  }

  protected abstract AbstractAlterTableDesc createDesc(ASTNode command, TableName tableName,
      Map<String, String> partitionSpec, Map<String, String> properties, boolean isToTxn, boolean isExplicitStatsUpdate,
      EnvironmentContext environmentContext) throws SemanticException;

  protected abstract boolean isView();
}
