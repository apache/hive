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

package org.apache.hadoop.hive.ql.anon.tez;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.mapred.JobConf;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ExtractWork extends BaseWork {
  private static final long serialVersionUID = 1L;

  private int numReducers = 1;
  private String inputDir;
  private String stagingDir;
  private final boolean indexFound;

  public ExtractWork(final boolean indexFound) {
    this.indexFound = indexFound;
  }

  public boolean isIndexFound() {
    return indexFound;
  }

  public void setNumReducers(final int numReducers) {
    this.numReducers = numReducers;
  }

  public int getNumReducers() {
    return numReducers;
  }

  public Path getOutputDir() {
    return stagingDir == null ? null : new Path(stagingDir);
  }

  @Override
  public void replaceRoots(final Map<Operator<?>, Operator<?>> replacementMap) {
  }

  public void setInputDir(final String inputDir) {
    this.inputDir = inputDir;
  }

  public String getInputDir() {
    return inputDir;
  }

  public void setStagingDir(final String stagingDir) {
    this.stagingDir = stagingDir;
  }

  public String getStagingDir() {
    return stagingDir;
  }

  @Override
  public Set<Operator<?>> getAllRootOperators() {
    return new HashSet<>();
  }

  @Override
  public Operator<? extends OperatorDesc> getAnyRootOperator() {
    return null;
  }

  @Override
  public void configureJobConf(final JobConf job) {
  }

  public int getMemory() {
    return 1024;
  }

  public int getCpu() {
    return 1;
  }

  public int getParallelism() {
    return indexFound ? 1 : -1;
  }
}
