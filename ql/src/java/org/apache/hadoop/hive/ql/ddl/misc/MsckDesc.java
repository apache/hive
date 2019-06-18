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
package org.apache.hadoop.hive.ql.ddl.misc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.DDLTask2;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for MSCK [REPAIR] TABLE ... [ADD|DROP|SYNC PARTITIONS] commands.
 */
@Explain(displayName = "Metastore Check", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class MsckDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  static {
    DDLTask2.registerOperation(MsckDesc.class, MsckOperation.class);
  }

  private final String tableName;
  private final ArrayList<LinkedHashMap<String, String>> partitionsSpecs;
  private final String resFile;
  private final boolean repairPartitions;
  private final boolean addPartitions;
  private final boolean dropPartitions;

  public MsckDesc(String tableName, List<? extends Map<String, String>> partitionSpecs, Path resFile,
      boolean repairPartitions, boolean addPartitions, boolean dropPartitions) {
    this.tableName = tableName;
    this.partitionsSpecs = new ArrayList<LinkedHashMap<String, String>>(partitionSpecs.size());
    for (Map<String, String> partSpec : partitionSpecs) {
      this.partitionsSpecs.add(new LinkedHashMap<>(partSpec));
    }
    this.resFile = resFile.toString();
    this.repairPartitions = repairPartitions;
    this.addPartitions = addPartitions;
    this.dropPartitions = dropPartitions;
  }

  @Explain(displayName = "table name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return tableName;
  }

  @Explain(displayName = "partitions specs", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public ArrayList<LinkedHashMap<String, String>> getPartitionsSpecs() {
    return partitionsSpecs;
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
