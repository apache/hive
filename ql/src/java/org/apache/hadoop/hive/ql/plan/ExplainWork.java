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
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.VectorizationDetailLevel;
import org.apache.hadoop.hive.ql.parse.ParseContext;

/**
 * ExplainWork.
 *
 */
public class ExplainWork implements Serializable {
  private static final long serialVersionUID = 1L;

  private Path resFile;
  private List<Task<?>> rootTasks;
  private Task<?> fetchTask;
  private ASTNode astTree;
  private String astStringTree;
  private Set<ReadEntity> inputs;
  private Set<WriteEntity> outputs;
  private ParseContext pCtx;

  private ExplainConfiguration config;

  private boolean appendTaskType;

  private String cboInfo;
  private String cboPlan;

  private String optimizedSQL;

  private transient BaseSemanticAnalyzer analyzer;

  public ExplainWork() {
  }

  public ExplainWork(Path resFile,
      ParseContext pCtx,
      List<Task<?>> rootTasks,
      Task<?> fetchTask,
      ASTNode astTree,
      BaseSemanticAnalyzer analyzer,
      ExplainConfiguration config,
      String cboInfo,
      String optimizedSQL,
      String cboPlan) {
    this.resFile = resFile;
    this.rootTasks = new ArrayList<Task<?>>(rootTasks);
    this.fetchTask = fetchTask;
    if(astTree != null) {
      this.astTree = astTree;
    }
    this.analyzer = analyzer;
    if (analyzer != null) {
      this.inputs = analyzer.getInputs();
    }
    if (analyzer != null) {
      this.outputs = analyzer.getAllOutputs();
    }
    this.pCtx = pCtx;
    this.cboInfo = cboInfo;
    this.optimizedSQL = optimizedSQL;
    this.cboPlan = cboPlan;
    this.config = config;
  }

  public Path getResFile() {
    return resFile;
  }

  public void setResFile(Path resFile) {
    this.resFile = resFile;
  }

  public List<Task<?>> getRootTasks() {
    return rootTasks;
  }

  public void setRootTasks(List<Task<?>> rootTasks) {
    this.rootTasks = rootTasks;
  }

  public Task<?> getFetchTask() {
    return fetchTask;
  }

  public void setFetchTask(Task<?> fetchTask) {
    this.fetchTask = fetchTask;
  }

  public Set<ReadEntity> getInputs() {
    return inputs;
  }

  public void setInputs(Set<ReadEntity> inputs) {
    this.inputs = inputs;
  }

  public Set<WriteEntity> getOutputs() {
    return outputs;
  }

  public void setOutputs(Set<WriteEntity> outputs) {
    this.outputs = outputs;
  }

  public ASTNode getAstTree() {
    return astTree;
  }

  public String getAstStringTree() {
    if (astStringTree == null) {
      astStringTree = astTree.dump();
    }
    return astStringTree;
  }

  public boolean getExtended() {
    return config.isExtended();
  }

  public boolean getDependency() {
    return config.isDependency();
  }

  public boolean isFormatted() {
    return config.isFormatted();
  }

  public boolean isVectorization() {
    return config.isVectorization();
  }

  public boolean isVectorizationOnly() {
    return config.isVectorizationOnly();
  }

  public VectorizationDetailLevel isVectorizationDetailLevel() {
    return config.getVectorizationDetailLevel();
  }

  public boolean isDebug() {
    return config.isDebug();
  }

  public ParseContext getParseContext() {
    return pCtx;
  }

  public void setParseContext(ParseContext pCtx) {
    this.pCtx = pCtx;
  }

  public boolean isCbo() {
    return config.isCbo();
  }

  public boolean isLogical() {
    return config.isLogical();
  }

  public boolean isAppendTaskType() {
    return appendTaskType;
  }

  public void setAppendTaskType(boolean appendTaskType) {
    this.appendTaskType = appendTaskType;
  }

  public boolean isAuthorize() {
    return config.isAuthorize();
  }

  public BaseSemanticAnalyzer getAnalyzer() {
    return analyzer;
  }

  public boolean isUserLevelExplain() {
    return config.isUserLevelExplain();
  }

  public String getCboInfo() {
    return cboInfo;
  }

  public void setCboInfo(String cboInfo) {
    this.cboInfo = cboInfo;
  }

  public String getOptimizedSQL() {
    return optimizedSQL;
  }

  public void setOptimizedSQL(String optimizedSQL) {
    this.optimizedSQL = optimizedSQL;
  }

  public String getCboPlan() {
    return cboPlan;
  }

  public void setCboPlan(String cboPlan) {
    this.cboPlan = cboPlan;
  }

  public ExplainConfiguration getConfig() {
    return config;
  }

  public void setConfig(ExplainConfiguration config) {
    this.config = config;
  }

  public boolean isLocks() {
    return config.isLocks();
  }
  public boolean isAst() {
    return config.isAst();
  }

  public boolean isDDL() {
    return config.isDDL();
  }
}
