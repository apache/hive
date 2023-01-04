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
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ImportSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.apache.hadoop.hive.ql.plan.BaseCopyWork;
import org.apache.hadoop.hive.ql.plan.DeferredWorkContext;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

import java.util.Collections;
import java.util.TreeMap;

/**
 * MoveWork.
 *
 */
@Explain(displayName = "Move Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class MoveWork implements Serializable, BaseCopyWork {
  private static final long serialVersionUID = 1L;
  private LoadTableDesc loadTableWork;
  private LoadFileDesc loadFileWork;
  private LoadMultiFilesDesc loadMultiFilesWork;
  private boolean checkFileFormat;
  private boolean srcLocal;
  private boolean needCleanTarget;
  private boolean isReplication;
  private String dumpDirectory;
  private transient ReplicationMetricCollector metricCollector;
  private boolean isCTAS;

  /**
   * ReadEntitites that are passed to the hooks.
   */
  protected Set<ReadEntity> inputs;
  /**
   * List of WriteEntities that are passed to the hooks.
   */
  protected Set<WriteEntity> outputs;

  /**
   * List of inserted partitions
   */
  protected List<Partition> movedParts;
  private boolean isInReplicationScope = false;

  public MoveWork() {
  }


  private MoveWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs) {
    this.inputs = inputs;
    this.outputs = outputs;
    this.needCleanTarget = true;
  }

  public MoveWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
      final LoadTableDesc loadTableWork, final LoadFileDesc loadFileWork,
      boolean checkFileFormat, boolean srcLocal) {
    this(inputs, outputs);
    if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
      Utilities.FILE_OP_LOGGER.trace("Creating MoveWork " + System.identityHashCode(this)
        + " with " + loadTableWork + "; " + loadFileWork);
    }
    this.loadTableWork = loadTableWork;
    this.loadFileWork = loadFileWork;
    this.checkFileFormat = checkFileFormat;
    this.srcLocal = srcLocal;
  }

  public MoveWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
      final LoadTableDesc loadTableWork, final LoadFileDesc loadFileWork,
      boolean checkFileFormat) {
    this(inputs, outputs, loadTableWork, loadFileWork, checkFileFormat, false);
  }

  public MoveWork(boolean isCTAS, Set<ReadEntity> inputs, Set<WriteEntity> outputs, final LoadTableDesc loadTableWork,
      final LoadFileDesc loadFileWork, boolean checkFileFormat) {
    this(inputs, outputs, loadTableWork, loadFileWork, checkFileFormat);
    this.isCTAS = isCTAS;
  }

  public MoveWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
                  final LoadTableDesc loadTableWork, final LoadFileDesc loadFileWork,
                  boolean checkFileFormat, String dumpRoot, ReplicationMetricCollector metricCollector,
                  boolean isReplication) {
    this(inputs, outputs, loadTableWork, loadFileWork, checkFileFormat, false);
    this.dumpDirectory = dumpRoot;
    this.metricCollector = metricCollector;
    this.isReplication = isReplication;
  }

  public MoveWork(final MoveWork o) {
    loadTableWork = o.getLoadTableWork();
    loadFileWork = o.getLoadFileWork();
    loadMultiFilesWork = o.getLoadMultiFilesWork();
    checkFileFormat = o.getCheckFileFormat();
    srcLocal = o.isSrcLocal();
    inputs = o.getInputs();
    outputs = o.getOutputs();
    needCleanTarget = o.needCleanTarget;
  }
  public boolean isCTAS() {
    return isCTAS;
  }

  @Explain(displayName = "tables", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
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

  public Set<ReadEntity> getInputs() {
    return inputs;
  }

  public Set<WriteEntity> getOutputs() {
    return outputs;
  }

  public void setInputs(Set<ReadEntity> inputs) {
    this.inputs = inputs;
  }

  public void setOutputs(Set<WriteEntity> outputs) {
    this.outputs = outputs;
  }

  public boolean isSrcLocal() {
    return srcLocal;
  }

  public void setSrcLocal(boolean srcLocal) {
    this.srcLocal = srcLocal;
  }

  public boolean isNeedCleanTarget() {
    return needCleanTarget;
  }

  public void setNeedCleanTarget(boolean needCleanTarget) {
    this.needCleanTarget = needCleanTarget;
  }

  public void setIsInReplicationScope(boolean isInReplicationScope) {
    this.isInReplicationScope = isInReplicationScope;
  }

  public ReplicationMetricCollector getMetricCollector() {
    return metricCollector;
  }

  public String getDumpDirectory() {
    return dumpDirectory;
  }

  public boolean isReplication() {
    return isReplication;
  }

  public boolean getIsInReplicationScope() {
    return this.isInReplicationScope;
  }

  public void initializeFromDeferredContext(DeferredWorkContext deferredContext) throws HiveException {
    if (!deferredContext.isCalculated()) {
      // Read metadata from metastore and populate the members of the context
      ImportSemanticAnalyzer.setupDeferredContextFromMetadata(deferredContext);
    }

    if (deferredContext.inReplScope && AcidUtils.isTransactionalTable(deferredContext.table)) {
      LoadMultiFilesDesc loadFilesWork = new LoadMultiFilesDesc(
          Collections.singletonList(deferredContext.destPath),
          Collections.singletonList(deferredContext.tgtPath),
          true, null, null);
      setMultiFilesDesc(loadFilesWork);
      setNeedCleanTarget(deferredContext.replace);
    } else {
      LoadTableDesc loadTableWork = new LoadTableDesc(
          deferredContext.loadPath, Utilities.getTableDesc(deferredContext.table), new TreeMap<>(), deferredContext.loadFileType, deferredContext.writeId);
      loadTableWork.setStmtId(deferredContext.stmtId);
      setLoadTableWork(loadTableWork);
    }
  }
}
