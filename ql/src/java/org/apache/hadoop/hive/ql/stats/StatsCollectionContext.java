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

package org.apache.hadoop.hive.ql.stats;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.ql.exec.Task;


@InterfaceAudience.Public
@InterfaceStability.Unstable
public class StatsCollectionContext {

  private final Configuration hiveConf;
  private Task task;
  private List<String> statsTmpDirs;
  private String contextSuffix;

  public List<String> getStatsTmpDirs() {
    return statsTmpDirs;
  }

  public void setStatsTmpDirs(List<String> statsTmpDirs) {
    this.statsTmpDirs = statsTmpDirs;
  }

  public void setStatsTmpDir(String statsTmpDir) {
    this.statsTmpDirs = statsTmpDir == null ? new ArrayList<String>() :
        Arrays.asList(new String[]{statsTmpDir});
  }

  public StatsCollectionContext(Configuration hiveConf) {
    super();
    this.hiveConf = hiveConf;
  }

  public Configuration getHiveConf() {
    return hiveConf;
  }

  public Task getTask() {
    return task;
  }

  public void setTask(Task task) {
    this.task = task;
  }

  public void setContextSuffix(String suffix) {
    this.contextSuffix = suffix;
  }

  public String getContextSuffix() {
    return contextSuffix;
  }
}
