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

package org.apache.hadoop.hive.ql.ddl.table.misc.touch;

import java.util.Map;

import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ALTER TABLE ... TOUCH [PARTITION ...] commands.
 */
@Explain(displayName = "Touch", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterTableTouchDesc implements DDLDesc {
  private final String tableName;
  private Map<String, String> partitionSpec;

  public AlterTableTouchDesc(String tableName, Map<String, String> partitionSpec) {
    this.tableName = tableName;
    this.partitionSpec = partitionSpec;
  }

  @Explain(displayName = "table name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return tableName;
  }

  @Explain(displayName = "partition spec", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Map<String, String> getPartitionSpec() {
    return partitionSpec;
  }
}
