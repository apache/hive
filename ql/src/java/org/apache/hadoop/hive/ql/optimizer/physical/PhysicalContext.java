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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.ParseContext;

/**
 * physical context used by physical resolvers.
 */
public class PhysicalContext {

  protected HiveConf conf;
  private ParseContext parseContext;
  private Context context;
  protected List<Task<? extends Serializable>> rootTasks;
  protected Task<? extends Serializable> fetchTask;

  public PhysicalContext(HiveConf conf, ParseContext parseContext,
      Context context, List<Task<? extends Serializable>> rootTasks,
      Task<? extends Serializable> fetchTask) {
    super();
    this.conf = conf;
    this.parseContext = parseContext;
    this.context = context;
    this.rootTasks = rootTasks;
    this.fetchTask = fetchTask;
  }

  public HiveConf getConf() {
    return conf;
  }

  public void setConf(HiveConf conf) {
    this.conf = conf;
  }

  public ParseContext getParseContext() {
    return parseContext;
  }

  public void setParseContext(ParseContext parseContext) {
    this.parseContext = parseContext;
  }

  public Context getContext() {
    return context;
  }

  public void setContext(Context context) {
    this.context = context;
  }

  public List<Task<? extends Serializable>> getRootTasks() {
    return rootTasks;
  }

  public void setRootTasks(List<Task<? extends Serializable>> rootTasks) {
    this.rootTasks = rootTasks;
  }

  public Task<? extends Serializable> getFetchTask() {
    return fetchTask;
  }

  public void setFetchTask(Task<? extends Serializable> fetchTask) {
    this.fetchTask = fetchTask;
  }

  public void addToRootTask(Task<? extends Serializable> tsk){
    rootTasks.add(tsk);
  }

  public void removeFromRootTask(Task<? extends Serializable> tsk){
    rootTasks.remove(tsk);
  }
}
