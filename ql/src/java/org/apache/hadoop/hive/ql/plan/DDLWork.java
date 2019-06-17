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

import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

import java.io.Serializable;
import java.util.HashSet;

/**
 * DDLWork.
 *
 */
public class DDLWork implements Serializable {
  private static final long serialVersionUID = 1L;

  // TODO: this can probably be replaced with much less code via dynamic dispatch and/or templates.
  private InsertCommitHookDesc insertCommitHookDesc;
  private MsckDesc msckDesc;

  private ShowConfDesc showConfDesc;

  private ReplRemoveFirstIncLoadPendFlagDesc replSetFirstIncLoadFlagDesc;

  boolean needLock = false;

  /**
   * ReadEntitites that are passed to the hooks.
   */
  protected HashSet<ReadEntity> inputs;
  /**
   * List of WriteEntities that are passed to the hooks.
   */
  protected HashSet<WriteEntity> outputs;
  private CacheMetadataDesc cacheMetadataDesc;

  public DDLWork() {
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs) {
    this.inputs = inputs;
    this.outputs = outputs;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      ShowConfDesc showConfDesc) {
    this(inputs, outputs);
    this.showConfDesc = showConfDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      MsckDesc checkDesc) {
    this(inputs, outputs);

    msckDesc = checkDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      CacheMetadataDesc cacheMetadataDesc) {
    this(inputs, outputs);
    this.cacheMetadataDesc = cacheMetadataDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
          InsertCommitHookDesc insertCommitHookDesc
  ) {
    this(inputs, outputs);
    this.insertCommitHookDesc = insertCommitHookDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
                 ReplRemoveFirstIncLoadPendFlagDesc replSetFirstIncLoadFlagDesc) {
    this(inputs, outputs);
    this.replSetFirstIncLoadFlagDesc = replSetFirstIncLoadFlagDesc;
  }

  /**
   * @return Metastore check description
   */
  public MsckDesc getMsckDesc() {
    return msckDesc;
  }

  public HashSet<ReadEntity> getInputs() {
    return inputs;
  }

  public HashSet<WriteEntity> getOutputs() {
    return outputs;
  }

  public boolean getNeedLock() {
    return needLock;
  }

  public void setNeedLock(boolean needLock) {
    this.needLock = needLock;
  }

  /**
   * @return information about the metadata to be cached
   */
  public CacheMetadataDesc getCacheMetadataDesc() {
    return this.cacheMetadataDesc;
  }

  public ShowConfDesc getShowConfDesc() {
    return showConfDesc;
  }

  @Explain(displayName = "Insert operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public InsertCommitHookDesc getInsertCommitHookDesc() {
    return insertCommitHookDesc;
  }

  public ReplRemoveFirstIncLoadPendFlagDesc getReplSetFirstIncLoadFlagDesc() {
    return replSetFirstIncLoadFlagDesc;
  }

  public void setReplSetFirstIncLoadFlagDesc(ReplRemoveFirstIncLoadPendFlagDesc replSetFirstIncLoadFlagDesc) {
    this.replSetFirstIncLoadFlagDesc = replSetFirstIncLoadFlagDesc;
  }
}
