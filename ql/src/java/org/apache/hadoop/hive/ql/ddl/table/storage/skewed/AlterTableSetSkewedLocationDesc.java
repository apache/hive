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

package org.apache.hadoop.hive.ql.ddl.table.storage.skewed;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ALTER TABLE ... SET SKEWED LOCATION commands.
 */
@Explain(displayName = "Set Skewed Location", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterTableSetSkewedLocationDesc extends AbstractAlterTableDesc {
  private static final long serialVersionUID = 1L;

  private final Map<List<String>, String> skewedLocations;

  public AlterTableSetSkewedLocationDesc(TableName tableName, Map<String, String> partitionSpec,
      Map<List<String>, String> skewedLocations) throws SemanticException {
    super(AlterTableType.SET_SKEWED_LOCATION, tableName, partitionSpec, null, false, false, null);
    this.skewedLocations = skewedLocations;
  }

  public Map<List<String>, String> getSkewedLocations() {
    return skewedLocations;
  }

  // for Explain only
  @Explain(displayName = "skewed locations", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public List<String> getSkewedLocationsExplain() {
    return skewedLocations.entrySet().stream()
        .map(e -> "(" + e.getKey() + ": " + e.getValue() + ")").collect(Collectors.toList());
  }

  @Override
  public boolean mayNeedWriteId() {
    return true;
  }
}
