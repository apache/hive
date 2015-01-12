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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
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
  // Temp HDFS path for Spark HashTable sink
  private Path tmpHDFSPath;

  private List<Operator<? extends OperatorDesc>> dummyParentOp;
  private Map<MapJoinOperator, List<Operator<? extends OperatorDesc>>> directFetchOp;

  private boolean hasStagedAlias;

  public MapredLocalWork() {
    this(new LinkedHashMap<String, Operator<? extends OperatorDesc>>(),
        new LinkedHashMap<String, FetchWork>());
    this.dummyParentOp = new ArrayList<Operator<? extends OperatorDesc>>();
    this.directFetchOp = new LinkedHashMap<MapJoinOperator, List<Operator<? extends OperatorDesc>>>();
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
    return dummyParentOp;
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

  public void setTmpHDFSPath(Path tmpPath) {
    this.tmpHDFSPath = tmpPath;
  }

  public Path getTmpHDFSPath() {
    return tmpHDFSPath;
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

  public MapredLocalWork extractDirectWorks(
      Map<MapJoinOperator, List<Operator<? extends OperatorDesc>>> directWorks) {
    MapredLocalWork newLocalWork = new MapredLocalWork();
    newLocalWork.setTmpPath(tmpPath);
    newLocalWork.setInputFileChangeSensitive(inputFileChangeSensitive);

    Set<Operator<?>> validWorks = getDirectWorks(directWorks.values());
    if (validWorks.isEmpty()) {
      // all small aliases are staged.. no need full bucket context
      newLocalWork.setBucketMapjoinContext(copyPartSpecMappingOnly());
      return newLocalWork;
    }
    newLocalWork.directFetchOp =
        new HashMap<MapJoinOperator, List<Operator<? extends OperatorDesc>>>(directWorks);
    newLocalWork.aliasToWork = new LinkedHashMap<String, Operator<? extends OperatorDesc>>();
    newLocalWork.aliasToFetchWork = new LinkedHashMap<String, FetchWork>();

    Map<String, Operator<?>> works = new HashMap<String, Operator<?>>(aliasToWork);
    for (Map.Entry<String, Operator<?>> entry : works.entrySet()) {
      String alias = entry.getKey();
      boolean notStaged = validWorks.contains(entry.getValue());
      newLocalWork.aliasToWork.put(alias, notStaged ? aliasToWork.remove(alias) : null);
      newLocalWork.aliasToFetchWork.put(alias, notStaged ? aliasToFetchWork.remove(alias) : null);
    }
    // copy full bucket context
    newLocalWork.setBucketMapjoinContext(getBucketMapjoinContext());
    return newLocalWork;
  }

  private Set<Operator<?>> getDirectWorks(Collection<List<Operator<?>>> values) {
    Set<Operator<?>> operators = new HashSet<Operator<?>>();
    for (List<Operator<?>> works : values) {
      for (Operator<?> work : works) {
        if (work != null) {
          operators.add(work);
        }
      }
    }
    return operators;
  }

  public void setDirectFetchOp(Map<MapJoinOperator, List<Operator<? extends OperatorDesc>>> op){
    this.directFetchOp = op;
  }

  public Map<MapJoinOperator, List<Operator<? extends OperatorDesc>>> getDirectFetchOp() {
    return directFetchOp;
  }

  public boolean hasStagedAlias() {
    return hasStagedAlias;
  }

  public void setHasStagedAlias(boolean hasStagedAlias) {
    this.hasStagedAlias = hasStagedAlias;
  }
}
