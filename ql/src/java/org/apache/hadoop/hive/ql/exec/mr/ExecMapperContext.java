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
package org.apache.hadoop.hive.ql.exec.mr;

import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FetchOperator;
import org.apache.hadoop.hive.ql.io.IOContext;
import org.apache.hadoop.hive.ql.io.IOContextMap;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.mapred.JobConf;

public class ExecMapperContext {

  // lastInputPath should be changed by the root of the operator tree ExecMapper.map()
  // but kept unchanged throughout the operator tree for one row
  private Path lastInputPath = null;

  // currentInputFile will be updated only by inputFileChanged(). If inputFileChanged()
  // is not called throughout the operator tree, currentInputPath won't be used anyways
  // so it won't be updated.
  private Path currentInputPath = null;
  private boolean inputFileChecked = false;

  // for SMB join, replaced with number part of task-id , making output file name
  // if big alias is not partitioned table, it's bucket number
  // if big alias is partitioned table, it's partition spec + bucket number
  private String fileId = null;
  private MapredLocalWork localWork = null;
  private Map<String, FetchOperator> fetchOperators;
  private JobConf jc;

  private IOContext ioCxt;

  private String currentBigBucketFile=null;

  public String getCurrentBigBucketFile() {
    return currentBigBucketFile;
  }

  public void setCurrentBigBucketFile(String currentBigBucketFile) {
    this.currentBigBucketFile = currentBigBucketFile;
  }

  public ExecMapperContext(JobConf jc) {
    this.jc = jc;
    ioCxt = IOContextMap.get(jc);
  }

  public void clear() {
    IOContextMap.clear();
    ioCxt = null;
  }

  /**
   * For CompbineFileInputFormat, the mapper's input file will be changed on the
   * fly, and the input file name is passed to jobConf by shims/initNextRecordReader.
   * If the map local work has any mapping depending on the current
   * mapper's input file, the work need to clear context and re-initialization
   * after the input file changed. This is first introduced to process bucket
   * map join.
   *
   * @return is the input file changed?
   */
  public boolean inputFileChanged() {
    if (!inputFileChecked) {
      currentInputPath = this.ioCxt.getInputPath();
      inputFileChecked = true;
    }
    return lastInputPath == null || !lastInputPath.equals(currentInputPath);
  }

  /**
   * Reset the execution context for each new row. This function should be called only
   * once at the root of the operator tree -- ExecMapper.map().
   * Note: this function should be kept minimum since it is called for each input row.
   */
  public void resetRow() {
    // Update the lastInputFile with the currentInputFile.
    lastInputPath = currentInputPath;
    inputFileChecked = false;
  }

  public Path getLastInputPath() {
    return lastInputPath;
  }

  public void setLastInputPath(Path lastInputPath) {
    this.lastInputPath = lastInputPath;
  }

  public Path getCurrentInputPath() {
    currentInputPath = this.ioCxt.getInputPath();
    return currentInputPath;
  }

  public void setCurrentInputPath(Path currentInputPath) {
    this.currentInputPath = currentInputPath;
  }

  public JobConf getJc() {
    return jc;
  }
  public void setJc(JobConf jc) {
    this.jc = jc;
  }

  public MapredLocalWork getLocalWork() {
    return localWork;
  }

  public void setLocalWork(MapredLocalWork localWork) {
    this.localWork = localWork;
  }

  public String getFileId() {
    return fileId;
  }

  public void setFileId(String fileId) {
    this.fileId = fileId;
  }

  public Map<String, FetchOperator> getFetchOperators() {
    return fetchOperators;
  }

  public void setFetchOperators(Map<String, FetchOperator> fetchOperators) {
    this.fetchOperators = fetchOperators;
  }

  public IOContext getIoCxt() {
    return ioCxt;
  }
}
