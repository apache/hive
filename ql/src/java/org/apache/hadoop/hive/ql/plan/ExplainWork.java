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
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
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
  private ArrayList<Task<? extends Serializable>> rootTasks;
  private Task<? extends Serializable> fetchTask;
  private HashSet<ReadEntity> inputs;
  private ParseContext pCtx;

  private ExplainConfiguration config;

  boolean appendTaskType;

  String cboInfo;

  private transient BaseSemanticAnalyzer analyzer;

  public ExplainWork() {
  }

  public ExplainWork(Path resFile,
      ParseContext pCtx,
      List<Task<? extends Serializable>> rootTasks,
      Task<? extends Serializable> fetchTask,
      BaseSemanticAnalyzer analyzer,
      ExplainConfiguration config,
      String cboInfo) {
    this.resFile = resFile;
    this.rootTasks = new ArrayList<Task<? extends Serializable>>(rootTasks);
    this.fetchTask = fetchTask;
    this.analyzer = analyzer;
    if (analyzer != null) {
      this.inputs = analyzer.getInputs();
    }
    this.pCtx = pCtx;
    this.cboInfo = cboInfo;
    this.config = config;
  }

  public Path getResFile() {
    return resFile;
  }

  public void setResFile(Path resFile) {
    this.resFile = resFile;
  }

  public ArrayList<Task<? extends Serializable>> getRootTasks() {
    return rootTasks;
  }

  public void setRootTasks(ArrayList<Task<? extends Serializable>> rootTasks) {
    this.rootTasks = rootTasks;
  }

  public Task<? extends Serializable> getFetchTask() {
    return fetchTask;
  }

  public void setFetchTask(Task<? extends Serializable> fetchTask) {
    this.fetchTask = fetchTask;
  }

  public HashSet<ReadEntity> getInputs() {
    return inputs;
  }

  public void setInputs(HashSet<ReadEntity> inputs) {
    this.inputs = inputs;
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

  public ParseContext getParseContext() {
    return pCtx;
  }

  public void setParseContext(ParseContext pCtx) {
    this.pCtx = pCtx;
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

  public ExplainConfiguration getConfig() {
    return config;
  }

  public void setConfig(ExplainConfiguration config) {
    this.config = config;
  }

}
