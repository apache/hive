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

import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.plan.Explain.Level;


/**
 * DropFunctionDesc.
 *
 */
@Explain(displayName = "Drop Function", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class DropFunctionDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  private String functionName;
  private boolean isTemp;
  private ReplicationSpec replicationSpec;

  /**
   * For serialization only.
   */
  public DropFunctionDesc() {
  }
  
  public DropFunctionDesc(String functionName, boolean isTemp, ReplicationSpec replicationSpec) {
    this.functionName = functionName;
    this.isTemp = isTemp;
    this.replicationSpec = replicationSpec;
  }

  @Explain(displayName = "name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getFunctionName() {
    return functionName;
  }

  public void setFunctionName(String functionName) {
    this.functionName = functionName;
  }

  public boolean isTemp() {
    return isTemp;
  }

  public void setTemp(boolean isTemp) {
    this.isTemp = isTemp;
  }

  /**
   * @return what kind of replication scope this create is running under.
   * This can result in a "DROP IF NEWER THAN" kind of semantic
   */
  public ReplicationSpec getReplicationSpec() {
    if (replicationSpec == null) {
      this.replicationSpec = new ReplicationSpec();
    }
    return this.replicationSpec;
  }
}
