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

package org.apache.hadoop.hive.ql.ddl.view;

import java.io.Serializable;

import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for all the ALTER MATERIALIZED VIEW commands.
 */
@Explain(displayName = "Alter Materialized View", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
abstract class AlterMaterializedViewDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * Alter Materialized View Types.
   */
  enum AlterMaterializedViewTypes {
    UPDATE_REWRITE_FLAG
  };

  private AlterMaterializedViewTypes op;

  AlterMaterializedViewDesc(AlterMaterializedViewTypes type) {
    this.op = type;
  }

  @Explain(displayName = "operation", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getOpString() {
    return op.toString();
  }
}
