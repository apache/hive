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
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ALTER TABLE ... UNSET TBLPROPERTIES [IF EXISTS] ... commands.
 */
@Explain(displayName = "Unset Properties", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterTableUnsetPropertiesDesc extends AbstractAlterTableDesc {
  private static final long serialVersionUID = 1L;

  private final boolean isExplicitStatsUpdate;
  private final EnvironmentContext environmentContext;

  public AlterTableUnsetPropertiesDesc(TableName tableName, Map<String, String> partitionSpec,
      ReplicationSpec replicationSpec, boolean expectView, Map<String, String> props, boolean isExplicitStatsUpdate,
      EnvironmentContext environmentContext) throws SemanticException {
    super(AlterTableType.DROPPROPS, tableName, partitionSpec, replicationSpec, false, expectView, props);
    this.isExplicitStatsUpdate = isExplicitStatsUpdate;
    this.environmentContext = environmentContext;
  }

  @Override
  public EnvironmentContext getEnvironmentContext() {
    return environmentContext;
  }

  @Override
  public boolean mayNeedWriteId() {
    return isExplicitStatsUpdate;
  }
}
