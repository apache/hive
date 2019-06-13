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

package org.apache.hadoop.hive.ql.plan;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

import java.io.Serializable;

/**
 * ReplRemoveFirstIncLoadPendFlagDesc. -- Remove the flag from db/table property if its already present.
 *
 */
@Explain(displayName = "Set First Incr Load Pend Flag", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ReplRemoveFirstIncLoadPendFlagDesc extends DDLDesc implements Serializable {

  private static final long serialVersionUID = 1L;
  String databaseName;
  String tableName;

  /**
   * For serialization only.
   */
  public ReplRemoveFirstIncLoadPendFlagDesc() {
  }

  public ReplRemoveFirstIncLoadPendFlagDesc(String databaseName) {
    super();
    this.databaseName = databaseName;
  }

  @Explain(displayName="db_name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }
}
