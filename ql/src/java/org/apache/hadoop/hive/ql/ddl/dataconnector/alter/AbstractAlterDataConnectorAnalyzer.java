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

package org.apache.hadoop.hive.ql.ddl.dataconnector.alter;

import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for data connector alteration commands.
 */
public abstract class AbstractAlterDataConnectorAnalyzer extends BaseSemanticAnalyzer {
  public AbstractAlterDataConnectorAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  protected void addAlterDataConnectorDesc(AbstractAlterDataConnectorDesc alterDesc) throws SemanticException {
    DataConnector connector = getDataConnector(alterDesc.getConnectorName());
    outputs.add(new WriteEntity(connector, WriteEntity.WriteType.DDL_NO_LOCK));
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), alterDesc)));
  }
}
