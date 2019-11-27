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
import org.apache.hadoop.hive.ql.ddl.table.constraint.Constraints;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Abstract ancestor of all ALTER TABLE descriptors that are handled by the AlterTableWithWriteIdOperations framework
 * and also has constraint changes.
 */
public abstract class AbstractAlterTableWithConstraintsDesc extends AbstractAlterTableDesc {
  private static final long serialVersionUID = 1L;

  private final Constraints constraints;

  public AbstractAlterTableWithConstraintsDesc(AlterTableType type, TableName tableName,
      Map<String, String> partitionSpec, ReplicationSpec replicationSpec, boolean isCascade, boolean expectView,
      Map<String, String> props, Constraints constraints) throws SemanticException {
    super(type, tableName, partitionSpec, replicationSpec, isCascade, expectView, props);
    this.constraints = constraints;
  }

  public Constraints getConstraints() {
    return constraints;
  }
}
