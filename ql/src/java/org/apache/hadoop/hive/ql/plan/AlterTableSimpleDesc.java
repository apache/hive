/**
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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.plan.AlterTableDesc.AlterTableTypes;

/**
 * Contains information needed to modify a partition or a table
 */
public class AlterTableSimpleDesc extends DDLDesc {
  private String tableName;
  private LinkedHashMap<String, String> partSpec;
  private String compactionType;
  private boolean isBlocking = false;

  AlterTableTypes type;
  private Map<String, String> props;

  public AlterTableSimpleDesc() {
  }

  /**
   * @param tableName
   *          table containing the partition
   * @param partSpec
   */
  public AlterTableSimpleDesc(String tableName,
      Map<String, String> partSpec, AlterTableTypes type) {
    this.tableName = tableName;
    if(partSpec == null) {
      this.partSpec = null;
    } else {
      this.partSpec = new LinkedHashMap<String,String>(partSpec);
    }
    this.type = type;
  }

  /**
   * Constructor for ALTER TABLE ... COMPACT.
   * @param tableName name of the table to compact
   * @param partSpec partition to compact
   * @param compactionType currently supported values: 'major' and 'minor'
   */
  public AlterTableSimpleDesc(String tableName,
      LinkedHashMap<String, String> partSpec, String compactionType, boolean isBlocking) {
    type = AlterTableTypes.COMPACT;
    this.compactionType = compactionType;
    this.tableName = tableName;
    this.partSpec = partSpec;
    this.isBlocking = isBlocking;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public AlterTableDesc.AlterTableTypes getType() {
    return type;
  }

  public void setType(AlterTableDesc.AlterTableTypes type) {
    this.type = type;
  }

  public LinkedHashMap<String, String> getPartSpec() {
    return partSpec;
  }

  public void setPartSpec(LinkedHashMap<String, String> partSpec) {
    this.partSpec = partSpec;
  }

  /**
   * Get what type of compaction is being done by a ALTER TABLE ... COMPACT statement.
   * @return Compaction type, currently supported values are 'major' and 'minor'.
   */
  public String getCompactionType() {
    return compactionType;
  }

  /**
   * if compaction request should block until completion
   */
  public boolean isBlocking() {
    return isBlocking;
  }

  public Map<String, String> getProps() {
    return props;
  }

  public void setProps(Map<String, String> props) {
    this.props = props;
  }
}
