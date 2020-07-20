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
package org.apache.hadoop.hive.ql.ddl.misc.msck;

import java.io.Serializable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for MSCK [REPAIR] TABLE ... [ADD|DROP|SYNC PARTITIONS] commands.
 */
@Explain(displayName = "Metastore Check", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class MsckDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  private final String tableName;
  private final byte[] filterExp;
  private final String resFile;
  private final boolean repairPartitions;
  private final boolean addPartitions;
  private final boolean dropPartitions;

  public MsckDesc(String tableName, byte[] filterExp, Path resFile,
                  boolean repairPartitions, boolean addPartitions, boolean dropPartitions) {
    this.tableName = tableName;
    this.filterExp = filterExp;
    this.resFile = resFile.toString();
    this.repairPartitions = repairPartitions;
    this.addPartitions = addPartitions;
    this.dropPartitions = dropPartitions;
  }

  @Explain(displayName = "table name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return tableName;
  }

  @Explain(displayName = "filter expression", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public byte[] getFilterExp() {
    return filterExp;
  }

  public String getResFile() {
    return resFile;
  }

  @Explain(displayName = "repair partition", displayOnlyOnTrue = true,
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isRepairPartitions() {
    return repairPartitions;
  }

  @Explain(displayName = "add partition", displayOnlyOnTrue = true,
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isAddPartitions() {
    return addPartitions;
  }

  @Explain(displayName = "drop partition", displayOnlyOnTrue = true,
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isDropPartitions() {
    return dropPartitions;
  }
}
