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
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Operator;

/**
 * MapredLocalWork.
 *
 */
@Explain(displayName = "Map Reduce Local Work")
public class MapredLocalWork implements Serializable {
  private static final long serialVersionUID = 1L;

  private LinkedHashMap<String, Operator<? extends OperatorDesc>> aliasToWork;
  private LinkedHashMap<String, FetchWork> aliasToFetchWork;
  private boolean inputFileChangeSensitive;
  private BucketMapJoinContext bucketMapjoinContext;
  private Path tmpPath;
  private String stageID;

  private List<Operator<? extends OperatorDesc>> dummyParentOp ;

  public MapredLocalWork() {

  }

  public MapredLocalWork(
    final LinkedHashMap<String, Operator<? extends OperatorDesc>> aliasToWork,
    final LinkedHashMap<String, FetchWork> aliasToFetchWork) {
    this.aliasToWork = aliasToWork;
    this.aliasToFetchWork = aliasToFetchWork;

  }

  public MapredLocalWork(MapredLocalWork clone){
    this.tmpPath = clone.tmpPath;
    this.inputFileChangeSensitive=clone.inputFileChangeSensitive;

  }


  public void setDummyParentOp(List<Operator<? extends OperatorDesc>> op){
    this.dummyParentOp=op;
  }


  public List<Operator<? extends OperatorDesc>> getDummyParentOp(){
    return this.dummyParentOp;
  }


  @Explain(displayName = "Alias -> Map Local Operator Tree")
  public LinkedHashMap<String, Operator<? extends OperatorDesc>> getAliasToWork() {
    return aliasToWork;
  }

  public String getStageID() {
    return stageID;
  }

  public void setStageID(String stageID) {
    this.stageID = stageID;
  }

  public void setAliasToWork(
    final LinkedHashMap<String, Operator<? extends OperatorDesc>> aliasToWork) {
    this.aliasToWork = aliasToWork;
  }

  /**
   * @return the aliasToFetchWork
   */
  @Explain(displayName = "Alias -> Map Local Tables")
  public LinkedHashMap<String, FetchWork> getAliasToFetchWork() {
    return aliasToFetchWork;
  }

  /**
   * @param aliasToFetchWork
   *          the aliasToFetchWork to set
   */
  public void setAliasToFetchWork(
      final LinkedHashMap<String, FetchWork> aliasToFetchWork) {
    this.aliasToFetchWork = aliasToFetchWork;
  }

  public boolean getInputFileChangeSensitive() {
    return inputFileChangeSensitive;
  }

  public void setInputFileChangeSensitive(boolean inputFileChangeSensitive) {
    this.inputFileChangeSensitive = inputFileChangeSensitive;
  }



  public void deriveExplainAttributes() {
    if (bucketMapjoinContext != null) {
      bucketMapjoinContext.deriveBucketMapJoinMapping();
    }
    for (FetchWork fetchWork : aliasToFetchWork.values()) {
      PlanUtils.configureInputJobPropertiesForStorageHandler(
        fetchWork.getTblDesc());
    }
  }

  @Explain(displayName = "Bucket Mapjoin Context", normalExplain = false)
  public BucketMapJoinContext getBucketMapjoinContextExplain() {
    return bucketMapjoinContext != null &&
        bucketMapjoinContext.getBucketFileNameMapping() != null ? bucketMapjoinContext : null;
  }

  public BucketMapJoinContext getBucketMapjoinContext() {
    return bucketMapjoinContext;
  }

  public void setBucketMapjoinContext(BucketMapJoinContext bucketMapjoinContext) {
    this.bucketMapjoinContext = bucketMapjoinContext;
  }

  public BucketMapJoinContext copyPartSpecMappingOnly() {
    if (bucketMapjoinContext != null &&
        bucketMapjoinContext.getBigTablePartSpecToFileMapping() != null) {
      BucketMapJoinContext context = new BucketMapJoinContext();
      context.setBigTablePartSpecToFileMapping(
          bucketMapjoinContext.getBigTablePartSpecToFileMapping());
      return context;
    }
    return null;
  }

  public void setTmpPath(Path tmpPath) {
    this.tmpPath = tmpPath;
  }

  public Path getTmpPath() {
    return tmpPath;
  }

  public String getBucketFileName(String bigFileName) {
    if (!inputFileChangeSensitive || bigFileName == null || bigFileName.isEmpty()) {
      return "-";
    }
    String fileName = getFileName(bigFileName);
    if (bucketMapjoinContext != null) {
      fileName = bucketMapjoinContext.createFileName(bigFileName, fileName);
    }
    return fileName;
  }

  private String getFileName(String path) {
    int last_separator = path.lastIndexOf(Path.SEPARATOR);
    if (last_separator < 0) {
      return path;
    }
    return path.substring(last_separator + 1);
  }
}
