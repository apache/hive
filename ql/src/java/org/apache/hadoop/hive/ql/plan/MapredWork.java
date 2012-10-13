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

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.SplitSample;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * MapredWork.
 *
 */
@Explain(displayName = "Map Reduce")
public class MapredWork extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;
  private String command;
  // map side work
  // use LinkedHashMap to make sure the iteration order is
  // deterministic, to ease testing
  private LinkedHashMap<String, ArrayList<String>> pathToAliases;

  private LinkedHashMap<String, PartitionDesc> pathToPartitionInfo;

  private LinkedHashMap<String, Operator<? extends OperatorDesc>> aliasToWork;

  private LinkedHashMap<String, PartitionDesc> aliasToPartnInfo;

  private HashMap<String, SplitSample> nameToSplitSample;

  // map<->reduce interface
  // schema of the map-reduce 'key' object - this is homogeneous
  private TableDesc keyDesc;

  // schema of the map-reduce 'val' object - this is heterogeneous
  private List<TableDesc> tagToValueDesc;

  private Operator<?> reducer;

  private Integer numReduceTasks;
  private Integer numMapTasks;
  private Long maxSplitSize;
  private Long minSplitSize;
  private Long minSplitSizePerNode;
  private Long minSplitSizePerRack;

  private boolean needsTagging;
  private boolean hadoopSupportsSplittable;

  private MapredLocalWork mapLocalWork;
  private String inputformat;
  private String indexIntermediateFile;
  private boolean gatheringStats;

  private String tmpHDFSFileURI;

  private LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtxMap;

  private QBJoinTree joinTree;

  private boolean mapperCannotSpanPartns;

  // used to indicate the input is sorted, and so a BinarySearchRecordReader shoudl be used
  private boolean inputFormatSorted = false;

  private transient boolean useBucketizedHiveInputFormat;

  public MapredWork() {
    aliasToPartnInfo = new LinkedHashMap<String, PartitionDesc>();
  }

  public MapredWork(
      final String command,
      final LinkedHashMap<String, ArrayList<String>> pathToAliases,
      final LinkedHashMap<String, PartitionDesc> pathToPartitionInfo,
      final LinkedHashMap<String, Operator<? extends OperatorDesc>> aliasToWork,
      final TableDesc keyDesc, List<TableDesc> tagToValueDesc,
      final Operator<?> reducer, final Integer numReduceTasks,
      final MapredLocalWork mapLocalWork,
      final boolean hadoopSupportsSplittable) {
    this.command = command;
    this.pathToAliases = pathToAliases;
    this.pathToPartitionInfo = pathToPartitionInfo;
    this.aliasToWork = aliasToWork;
    this.keyDesc = keyDesc;
    this.tagToValueDesc = tagToValueDesc;
    this.reducer = reducer;
    this.numReduceTasks = numReduceTasks;
    this.mapLocalWork = mapLocalWork;
    aliasToPartnInfo = new LinkedHashMap<String, PartitionDesc>();
    this.hadoopSupportsSplittable = hadoopSupportsSplittable;
    maxSplitSize = null;
    minSplitSize = null;
    minSplitSizePerNode = null;
    minSplitSizePerRack = null;
  }

  public String getCommand() {
    return command;
  }

  public void setCommand(final String command) {
    this.command = command;
  }

  @Explain(displayName = "Path -> Alias", normalExplain = false)
  public LinkedHashMap<String, ArrayList<String>> getPathToAliases() {
    return pathToAliases;
  }

  public void setPathToAliases(
      final LinkedHashMap<String, ArrayList<String>> pathToAliases) {
    this.pathToAliases = pathToAliases;
  }

  @Explain(displayName = "Truncated Path -> Alias", normalExplain = false)
  /**
   * This is used to display and verify output of "Path -> Alias" in test framework.
   *
   * {@link QTestUtil} masks "Path -> Alias" and makes verification impossible.
   * By keeping "Path -> Alias" intact and adding a new display name which is not
   * masked by {@link QTestUtil} by removing prefix.
   *
   * Notes: we would still be masking for intermediate directories.
   *
   * @return
   */
  public Map<String, ArrayList<String>> getTruncatedPathToAliases() {
    Map<String, ArrayList<String>> trunPathToAliases = new LinkedHashMap<String,
        ArrayList<String>>();
    Iterator<Entry<String, ArrayList<String>>> itr = this.pathToAliases.entrySet().iterator();
    while (itr.hasNext()) {
      final Entry<String, ArrayList<String>> entry = itr.next();
      String origiKey = entry.getKey();
      String newKey = removePrefixFromWarehouseConfig(origiKey);
      ArrayList<String> value = entry.getValue();
      trunPathToAliases.put(newKey, value);
    }
    return trunPathToAliases;
  }

  /**
   * Remove prefix from "Path -> Alias"
   *
   * @param origiKey
   * @return
   */
  private String removePrefixFromWarehouseConfig(String origiKey) {
    String prefix = SessionState.get().getConf().getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
    if ((prefix != null) && (prefix.length() > 0)) {
      //Local file system is using pfile:/// {@link ProxyLocalFileSystem}
      prefix = prefix.replace("pfile:///", "pfile:/");
      int index = origiKey.indexOf(prefix);
      if (index > -1) {
        origiKey = origiKey.substring(index + prefix.length());
      }
    }
    return origiKey;
  }


  @Explain(displayName = "Path -> Partition", normalExplain = false)
  public LinkedHashMap<String, PartitionDesc> getPathToPartitionInfo() {
    return pathToPartitionInfo;
  }

  public void setPathToPartitionInfo(
      final LinkedHashMap<String, PartitionDesc> pathToPartitionInfo) {
    this.pathToPartitionInfo = pathToPartitionInfo;
  }

  /**
   * @return the aliasToPartnInfo
   */
  public LinkedHashMap<String, PartitionDesc> getAliasToPartnInfo() {
    return aliasToPartnInfo;
  }

  /**
   * @param aliasToPartnInfo
   *          the aliasToPartnInfo to set
   */
  public void setAliasToPartnInfo(
      LinkedHashMap<String, PartitionDesc> aliasToPartnInfo) {
    this.aliasToPartnInfo = aliasToPartnInfo;
  }

  @Explain(displayName = "Alias -> Map Operator Tree")
  public LinkedHashMap<String, Operator<? extends OperatorDesc>> getAliasToWork() {
    return aliasToWork;
  }

  public void setAliasToWork(
      final LinkedHashMap<String, Operator<? extends OperatorDesc>> aliasToWork) {
    this.aliasToWork = aliasToWork;
  }

  /**
   * @return the mapredLocalWork
   */
  @Explain(displayName = "Local Work")
  public MapredLocalWork getMapLocalWork() {
    return mapLocalWork;
  }

  /**
   * @param mapLocalWork
   *          the mapredLocalWork to set
   */
  public void setMapLocalWork(final MapredLocalWork mapLocalWork) {
    this.mapLocalWork = mapLocalWork;
  }

  public TableDesc getKeyDesc() {
    return keyDesc;
  }

  public void setKeyDesc(final TableDesc keyDesc) {
    this.keyDesc = keyDesc;
  }

  public List<TableDesc> getTagToValueDesc() {
    return tagToValueDesc;
  }

  public void setTagToValueDesc(final List<TableDesc> tagToValueDesc) {
    this.tagToValueDesc = tagToValueDesc;
  }

  @Explain(displayName = "Reduce Operator Tree")
  public Operator<?> getReducer() {
    return reducer;
  }

  @Explain(displayName = "Percentage Sample")
  public HashMap<String, SplitSample> getNameToSplitSample() {
    return nameToSplitSample;
  }

  public void setNameToSplitSample(HashMap<String, SplitSample> nameToSplitSample) {
    this.nameToSplitSample = nameToSplitSample;
  }

  public void setReducer(final Operator<?> reducer) {
    this.reducer = reducer;
  }

  public Integer getNumMapTasks() {
    return numMapTasks;
  }

  public void setNumMapTasks(Integer numMapTasks) {
    this.numMapTasks = numMapTasks;
  }

  /**
   * If the number of reducers is -1, the runtime will automatically figure it
   * out by input data size.
   *
   * The number of reducers will be a positive number only in case the target
   * table is bucketed into N buckets (through CREATE TABLE). This feature is
   * not supported yet, so the number of reducers will always be -1 for now.
   */
  public Integer getNumReduceTasks() {
    return numReduceTasks;
  }

  public void setNumReduceTasks(final Integer numReduceTasks) {
    this.numReduceTasks = numReduceTasks;
  }

  @SuppressWarnings("nls")
  public void addMapWork(String path, String alias, Operator<?> work,
      PartitionDesc pd) {
    ArrayList<String> curAliases = pathToAliases.get(path);
    if (curAliases == null) {
      assert (pathToPartitionInfo.get(path) == null);
      curAliases = new ArrayList<String>();
      pathToAliases.put(path, curAliases);
      pathToPartitionInfo.put(path, pd);
    } else {
      assert (pathToPartitionInfo.get(path) != null);
    }

    for (String oneAlias : curAliases) {
      if (oneAlias.equals(alias)) {
        throw new RuntimeException("Multiple aliases named: " + alias
            + " for path: " + path);
      }
    }
    curAliases.add(alias);

    if (aliasToWork.get(alias) != null) {
      throw new RuntimeException("Existing work for alias: " + alias);
    }
    aliasToWork.put(alias, work);
  }

  @SuppressWarnings("nls")
  public String isInvalid() {
    if ((getNumReduceTasks() >= 1) && (getReducer() == null)) {
      return "Reducers > 0 but no reduce operator";
    }

    if ((getNumReduceTasks() == 0) && (getReducer() != null)) {
      return "Reducers == 0 but reduce operator specified";
    }

    return null;
  }

  public String toXML() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Utilities.serializeMapRedWork(this, baos);
    return (baos.toString());
  }

  // non bean

  /**
   * For each map side operator - stores the alias the operator is working on
   * behalf of in the operator runtime state. This is used by reducesink
   * operator - but could be useful for debugging as well.
   */
  private void setAliases() {
    if(aliasToWork == null) {
      return;
    }
    for (String oneAlias : aliasToWork.keySet()) {
      aliasToWork.get(oneAlias).setAlias(oneAlias);
    }
  }

  /**
   * Derive additional attributes to be rendered by EXPLAIN.
   */
  public void deriveExplainAttributes() {
    if (pathToPartitionInfo != null) {
      for (Map.Entry<String, PartitionDesc> entry : pathToPartitionInfo
          .entrySet()) {
        entry.getValue().deriveBaseFileName(entry.getKey());
      }
    }
    if (mapLocalWork != null) {
      mapLocalWork.deriveExplainAttributes();
    }
  }

  public void initialize() {
    setAliases();
  }

  @Explain(displayName = "Needs Tagging", normalExplain = false)
  public boolean getNeedsTagging() {
    return needsTagging;
  }

  public void setNeedsTagging(boolean needsTagging) {
    this.needsTagging = needsTagging;
  }

  public boolean getHadoopSupportsSplittable() {
    return hadoopSupportsSplittable;
  }

  public void setHadoopSupportsSplittable(boolean hadoopSupportsSplittable) {
    this.hadoopSupportsSplittable = hadoopSupportsSplittable;
  }

  public Long getMaxSplitSize() {
    return maxSplitSize;
  }

  public void setMaxSplitSize(Long maxSplitSize) {
    this.maxSplitSize = maxSplitSize;
  }

  public Long getMinSplitSize() {
    return minSplitSize;
  }

  public void setMinSplitSize(Long minSplitSize) {
    this.minSplitSize = minSplitSize;
  }

  public Long getMinSplitSizePerNode() {
    return minSplitSizePerNode;
  }

  public void setMinSplitSizePerNode(Long minSplitSizePerNode) {
    this.minSplitSizePerNode = minSplitSizePerNode;
  }

  public Long getMinSplitSizePerRack() {
    return minSplitSizePerRack;
  }

  public void setMinSplitSizePerRack(Long minSplitSizePerRack) {
    this.minSplitSizePerRack = minSplitSizePerRack;
  }

  public String getInputformat() {
    return inputformat;
  }

  public void setInputformat(String inputformat) {
    this.inputformat = inputformat;
  }

  public String getIndexIntermediateFile() {
    return indexIntermediateFile;
  }

  public void addIndexIntermediateFile(String fileName) {
    if (this.indexIntermediateFile == null) {
      this.indexIntermediateFile = fileName;
    } else {
      this.indexIntermediateFile += "," + fileName;
    }
  }

  public void setGatheringStats(boolean gatherStats) {
    this.gatheringStats = gatherStats;
  }

  public boolean isGatheringStats() {
    return this.gatheringStats;
  }

  public void setMapperCannotSpanPartns(boolean mapperCannotSpanPartns) {
    this.mapperCannotSpanPartns = mapperCannotSpanPartns;
  }

  public boolean isMapperCannotSpanPartns() {
    return this.mapperCannotSpanPartns;
  }

  public String getTmpHDFSFileURI() {
    return tmpHDFSFileURI;
  }

  public void setTmpHDFSFileURI(String tmpHDFSFileURI) {
    this.tmpHDFSFileURI = tmpHDFSFileURI;
  }


  public QBJoinTree getJoinTree() {
    return joinTree;
  }

  public void setJoinTree(QBJoinTree joinTree) {
    this.joinTree = joinTree;
  }

  public
    LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> getOpParseCtxMap() {
    return opParseCtxMap;
  }

  public void setOpParseCtxMap(
    LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtxMap) {
    this.opParseCtxMap = opParseCtxMap;
  }

  public boolean isInputFormatSorted() {
    return inputFormatSorted;
  }

  public void setInputFormatSorted(boolean inputFormatSorted) {
    this.inputFormatSorted = inputFormatSorted;
  }

  public void resolveDynamicPartitionMerge(HiveConf conf, Path path,
      TableDesc tblDesc, ArrayList<String> aliases, PartitionDesc partDesc) {
    pathToAliases.put(path.toString(), aliases);
    pathToPartitionInfo.put(path.toString(), partDesc);
  }

  public List<Operator<?>> getAllOperators() {
    ArrayList<Operator<?>> opList = new ArrayList<Operator<?>>();
    ArrayList<Operator<?>> returnList = new ArrayList<Operator<?>>();

    if (getReducer() != null) {
      opList.add(getReducer());
    }

    Map<String, ArrayList<String>> pa = getPathToAliases();
    if (pa != null) {
      for (List<String> ls : pa.values()) {
        for (String a : ls) {
          Operator<?> op = getAliasToWork().get(a);
          if (op != null ) {
            opList.add(op);
          }
        }
      }
    }

    //recursively add all children
    while (!opList.isEmpty()) {
      Operator<?> op = opList.remove(0);
      if (op.getChildOperators() != null) {
        opList.addAll(op.getChildOperators());
      }
      returnList.add(op);
    }

    return returnList;
  }

  public boolean isUseBucketizedHiveInputFormat() {
    return useBucketizedHiveInputFormat;
  }

  public void setUseBucketizedHiveInputFormat(boolean useBucketizedHiveInputFormat) {
    this.useBucketizedHiveInputFormat = useBucketizedHiveInputFormat;
  }
}
