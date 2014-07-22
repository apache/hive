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
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;

/**
 * ExplainWork.
 *
 */
public class ExplainWork implements Serializable {
  private static final long serialVersionUID = 1L;

  private Path resFile;
  private ArrayList<Task<? extends Serializable>> rootTasks;
  private Task<? extends Serializable> fetchTask;
  private String astStringTree;
  private HashSet<ReadEntity> inputs;
  private ParseContext pCtx;

  boolean extended;
  boolean formatted;
  boolean dependency;
  boolean logical;

  boolean appendTaskType;

  boolean authorize;

  private transient BaseSemanticAnalyzer analyzer;

  public ExplainWork() {
  }

  public ExplainWork(Path resFile,
      ParseContext pCtx,
      List<Task<? extends Serializable>> rootTasks,
      Task<? extends Serializable> fetchTask,
      String astStringTree,
      BaseSemanticAnalyzer analyzer,
      boolean extended,
      boolean formatted,
      boolean dependency,
      boolean logical,
      boolean authorize) {
    this.resFile = resFile;
    this.rootTasks = new ArrayList<Task<? extends Serializable>>(rootTasks);
    this.fetchTask = fetchTask;
    this.astStringTree = astStringTree;
    this.analyzer = analyzer;
    this.inputs = analyzer.getInputs();
    this.extended = extended;
    this.formatted = formatted;
    this.dependency = dependency;
    this.logical = logical;
    this.pCtx = pCtx;
    this.authorize = authorize;
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

  public String getAstStringTree() {
    return astStringTree;
  }

  public void setAstStringTree(String astStringTree) {
    this.astStringTree = astStringTree;
  }

  public HashSet<ReadEntity> getInputs() {
    return inputs;
  }

  public void setInputs(HashSet<ReadEntity> inputs) {
    this.inputs = inputs;
  }

  public boolean getExtended() {
    return extended;
  }

  public void setExtended(boolean extended) {
    this.extended = extended;
  }

  public boolean getDependency() {
    return dependency;
  }

  public void setDependency(boolean dependency) {
    this.dependency = dependency;
  }

  public boolean isFormatted() {
    return formatted;
  }

  public void setFormatted(boolean formatted) {
    this.formatted = formatted;
  }

  public ParseContext getParseContext() {
    return pCtx;
  }

  public void setParseContext(ParseContext pCtx) {
    this.pCtx = pCtx;
  }

  public boolean isLogical() {
    return logical;
  }

  public void setLogical(boolean logical) {
    this.logical = logical;
  }

  public boolean isAppendTaskType() {
    return appendTaskType;
  }

  public void setAppendTaskType(boolean appendTaskType) {
    this.appendTaskType = appendTaskType;
  }

  public boolean isAuthorize() {
    return authorize;
  }

  public void setAuthorize(boolean authorize) {
    this.authorize = authorize;
  }

  public BaseSemanticAnalyzer getAnalyzer() {
    return analyzer;
  }
}
