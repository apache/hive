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

import org.apache.hadoop.hive.ql.plan.DDLDesc.DDLDescWithWriteId;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * AlterMaterializedViewDesc.
 */
@Explain(displayName = "Alter Materialized View", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterMaterializedViewDesc extends DDLDesc implements Serializable, DDLDescWithWriteId {
  private static final long serialVersionUID = 1L;
  private String fqMaterializedViewName;
  private boolean rewriteEnable;

  /**
   * alterMVTypes.
   *
   */
  public static enum AlterMaterializedViewTypes {
    UPDATE_REWRITE_FLAG
  };

  AlterMaterializedViewTypes op;
  private long writeId;

  public AlterMaterializedViewDesc() {
  }

  public AlterMaterializedViewDesc(AlterMaterializedViewTypes type) {
    this.op = type;
  }

  /**
   * @return the name of the materializedViewName
   */
  @Explain(displayName = "name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getMaterializedViewName() {
    return fqMaterializedViewName;
  }

  /**
   * @param materializedViewName
   *          the materializedViewName to set
   */
  public void setFqMaterializedViewName(String materializedViewName) {
    this.fqMaterializedViewName = materializedViewName;
  }

  /**
   * @return the rewrite flag
   */
  public boolean isRewriteEnable() {
    return rewriteEnable;
  }

  /**
   * @param rewriteEnable
   *          the value for the flag
   */
  public void setRewriteEnableFlag(boolean rewriteEnable) {
    this.rewriteEnable = rewriteEnable;
  }

  /**
   * @return the op
   */
  @Explain(displayName = "operation", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getOpString() {
    return op.toString();
  }

  /**
   * @return the op
   */
  public AlterMaterializedViewTypes getOp() {
    return op;
  }

  /**
   * @param op
   *          the op to set
   */
  public void setOp(AlterMaterializedViewTypes op) {
    this.op = op;
  }

  @Override
  public void setWriteId(long writeId) {
    this.writeId = writeId;
  }

  @Override
  public String getFullTableName() {
    return fqMaterializedViewName;
  }

  @Override
  public boolean mayNeedWriteId() {
    return true; // Verified when this is set as DDL Desc for ACID.
  }

}
