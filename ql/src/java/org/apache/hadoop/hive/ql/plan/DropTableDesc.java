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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DropTableDesc.
 * TODO: this is currently used for both drop table and drop partitions.
 */
@Explain(displayName = "Drop Table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class DropTableDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  public static class PartSpec {
    public PartSpec(ExprNodeGenericFuncDesc partSpec, int prefixLength) {
      this.partSpec = partSpec;
      this.prefixLength = prefixLength;
    }
    public ExprNodeGenericFuncDesc getPartSpec() {
      return partSpec;
    }
    public int getPrefixLength() {
      return prefixLength;
    }
    private static final long serialVersionUID = 1L;
    private ExprNodeGenericFuncDesc partSpec;
    // TODO: see if we can get rid of this... used in one place to distinguish archived parts
    private int prefixLength;
  }

  String tableName;
  ArrayList<PartSpec> partSpecs;
  TableType expectedType;
  boolean ifExists;
  boolean ifPurge;
  ReplicationSpec replicationSpec;
  

  public DropTableDesc() {
  }

  /**
   * @param tableName
   * @param ifPurge
   */
  public DropTableDesc(
      String tableName, TableType expectedType, boolean ifExists,
      boolean ifPurge, ReplicationSpec replicationSpec) {
    this.tableName = tableName;
    this.partSpecs = null;
    this.expectedType = expectedType;
    this.ifExists = ifExists;
    this.ifPurge = ifPurge;
    this.replicationSpec = replicationSpec;
  }

  public DropTableDesc(String tableName, Map<Integer, List<ExprNodeGenericFuncDesc>> partSpecs,
      TableType expectedType, boolean ifPurge, ReplicationSpec replicationSpec) {
    this.tableName = tableName;
    this.partSpecs = new ArrayList<PartSpec>(partSpecs.size());
    for (Map.Entry<Integer, List<ExprNodeGenericFuncDesc>> partSpec : partSpecs.entrySet()) {
      int prefixLength = partSpec.getKey();
      for (ExprNodeGenericFuncDesc expr : partSpec.getValue()) {
        this.partSpecs.add(new PartSpec(expr, prefixLength));
      }
    }
    this.expectedType = expectedType;
    this.ifPurge = ifPurge;
    this.replicationSpec = replicationSpec;
  }

  /**
   * @return the tableName
   */
  @Explain(displayName = "table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName
   *          the tableName to set
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * @return the partSpecs
   */
  public ArrayList<PartSpec> getPartSpecs() {
    return partSpecs;
  }

  /**
   * @return whether to expect a view being dropped
   */
  public boolean getExpectView() {
    return expectedType != null && expectedType == TableType.VIRTUAL_VIEW;
  }

  /**
   * @return whether to expect a materialized view being dropped
   */
  public boolean getExpectMaterializedView() {
    return expectedType != null && expectedType == TableType.MATERIALIZED_VIEW;
  }

  /**
   * @return whether IF EXISTS was specified
   */
  public boolean getIfExists() {
    return ifExists;
  }

  /**
   * @param ifExists
   *          set whether IF EXISTS was specified
   */
  public void setIfExists(boolean ifExists) {
    this.ifExists = ifExists;
  }

  /**
   *  @return whether Purge was specified
   */
  public boolean getIfPurge() {
      return ifPurge;
  }

  /**
   * @param ifPurge
   *          set whether Purge was specified
   */
  public void setIfPurge(boolean ifPurge) {
      this.ifPurge = ifPurge;
  }

  /**
   * @return what kind of replication scope this drop is running under.
   * This can result in a "DROP IF OLDER THAN" kind of semantic
   */
  public ReplicationSpec getReplicationSpec(){
    if (replicationSpec == null){
      this.replicationSpec = new ReplicationSpec();
    }
    return this.replicationSpec;
  }
}
