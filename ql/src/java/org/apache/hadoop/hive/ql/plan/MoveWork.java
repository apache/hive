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
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;

/**
 * MoveWork.
 *
 */
@Explain(displayName = "Move Operator")
public class MoveWork implements Serializable {
  private static final long serialVersionUID = 1L;
  private LoadTableDesc loadTableWork;
  private LoadFileDesc loadFileWork;
  private LoadMultiFilesDesc loadMultiFilesWork;

  private boolean checkFileFormat;
  private boolean srcLocal;

  /**
   * ReadEntitites that are passed to the hooks.
   */
  protected HashSet<ReadEntity> inputs;
  /**
   * List of WriteEntities that are passed to the hooks.
   */
  protected HashSet<WriteEntity> outputs;

  /**
   * List of inserted partitions
   */
  protected List<Partition> movedParts;

  public MoveWork() {
  }

  public MoveWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs) {
    this.inputs = inputs;
    this.outputs = outputs;
  }

  public MoveWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      final LoadTableDesc loadTableWork, final LoadFileDesc loadFileWork,
      boolean checkFileFormat, boolean srcLocal) {
    this(inputs, outputs);
    this.loadTableWork = loadTableWork;
    this.loadFileWork = loadFileWork;
    this.checkFileFormat = checkFileFormat;
    this.srcLocal = srcLocal;
  }

  public MoveWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      final LoadTableDesc loadTableWork, final LoadFileDesc loadFileWork,
      boolean checkFileFormat) {
    this(inputs, outputs);
    this.loadTableWork = loadTableWork;
    this.loadFileWork = loadFileWork;
    this.checkFileFormat = checkFileFormat;
  }

  @Explain(displayName = "tables")
  public LoadTableDesc getLoadTableWork() {
    return loadTableWork;
  }

  public void setLoadTableWork(final LoadTableDesc loadTableWork) {
    this.loadTableWork = loadTableWork;
  }

  @Explain(displayName = "files")
  public LoadFileDesc getLoadFileWork() {
    return loadFileWork;
  }

  @Explain(displayName = "files")
  public LoadMultiFilesDesc getLoadMultiFilesWork() {
    return loadMultiFilesWork;
  }

  public void setMultiFilesDesc(LoadMultiFilesDesc lmfd) {
    this.loadMultiFilesWork = lmfd;
  }

  public void setLoadFileWork(final LoadFileDesc loadFileWork) {
    this.loadFileWork = loadFileWork;
  }

  public boolean getCheckFileFormat() {
    return checkFileFormat;
  }

  public void setCheckFileFormat(boolean checkFileFormat) {
    this.checkFileFormat = checkFileFormat;
  }

  public HashSet<ReadEntity> getInputs() {
    return inputs;
  }

  public HashSet<WriteEntity> getOutputs() {
    return outputs;
  }

  public void setInputs(HashSet<ReadEntity> inputs) {
    this.inputs = inputs;
  }

  public void setOutputs(HashSet<WriteEntity> outputs) {
    this.outputs = outputs;
  }

  public boolean isSrcLocal() {
    return srcLocal;
  }

  public void setSrcLocal(boolean srcLocal) {
    this.srcLocal = srcLocal;
  }

}
