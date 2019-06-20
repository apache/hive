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
package org.apache.hadoop.hive.ql.ddl;

import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

import java.io.Serializable;

import java.util.Set;

/**
 * A DDL operation.
 */
public final class DDLWork implements Serializable {
  private static final long serialVersionUID = 1L;

  private DDLDesc ddlDesc;
  boolean needLock = false;

  /** ReadEntitites that are passed to the hooks. */
  private Set<ReadEntity> inputs;
  /** List of WriteEntities that are passed to the hooks. */
  private Set<WriteEntity> outputs;

  public DDLWork() {
  }

  public DDLWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs) {
    this.inputs = inputs;
    this.outputs = outputs;
  }

  public DDLWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs, DDLDesc ddlDesc) {
    this(inputs, outputs);
    this.ddlDesc = ddlDesc;
  }

  public Set<ReadEntity> getInputs() {
    return inputs;
  }

  public Set<WriteEntity> getOutputs() {
    return outputs;
  }

  public boolean getNeedLock() {
    return needLock;
  }

  public void setNeedLock(boolean needLock) {
    this.needLock = needLock;
  }

  @Explain(skipHeader = true, explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public DDLDesc getDDLDesc() {
    return ddlDesc;
  }
}
