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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DropPartitionDesc.
 */
@Explain(displayName = "Drop Partition", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class DropPartitionDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * PartSpec.
   */
  public static class PartSpec implements Serializable {
    private static final long serialVersionUID = 1L;

    private ExprNodeGenericFuncDesc partSpec;
    // TODO: see if we can get rid of this... used in one place to distinguish archived parts
    private int prefixLength;

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
  }

  private final String tableName;
  private final ArrayList<PartSpec> partSpecs;
  private final boolean ifPurge;
  private final ReplicationSpec replicationSpec;

  public DropPartitionDesc(String tableName, Map<Integer, List<ExprNodeGenericFuncDesc>> partSpecs, boolean ifPurge,
      ReplicationSpec replicationSpec) {
    this.tableName = tableName;
    this.partSpecs = new ArrayList<PartSpec>(partSpecs.size());
    for (Map.Entry<Integer, List<ExprNodeGenericFuncDesc>> partSpec : partSpecs.entrySet()) {
      int prefixLength = partSpec.getKey();
      for (ExprNodeGenericFuncDesc expr : partSpec.getValue()) {
        this.partSpecs.add(new PartSpec(expr, prefixLength));
      }
    }
    this.ifPurge = ifPurge;
    this.replicationSpec = replicationSpec == null ? new ReplicationSpec() : replicationSpec;
  }

  @Explain(displayName = "table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return tableName;
  }

  public ArrayList<PartSpec> getPartSpecs() {
    return partSpecs;
  }

  public boolean getIfPurge() {
      return ifPurge;
  }

  /**
   * @return what kind of replication scope this drop is running under.
   * This can result in a "DROP IF OLDER THAN" kind of semantic
   */
  public ReplicationSpec getReplicationSpec(){
    return replicationSpec;
  }
}
