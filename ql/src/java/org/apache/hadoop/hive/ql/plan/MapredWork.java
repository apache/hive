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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;

/**
 * MapredWork.
 *
 */
@Explain(displayName = "Map Reduce")
public class MapredWork implements Serializable {
  private static final long serialVersionUID = 1L;
  private String command;
  // map side work
  // use LinkedHashMap to make sure the iteration order is
  // deterministic, to ease testing
  private LinkedHashMap<String, ArrayList<String>> pathToAliases;

  private LinkedHashMap<String, PartitionDesc> pathToPartitionInfo;

  private LinkedHashMap<String, Operator<? extends Serializable>> aliasToWork;

  private LinkedHashMap<String, PartitionDesc> aliasToPartnInfo;

  // map<->reduce interface
  // schema of the map-reduce 'key' object - this is homogeneous
  private TableDesc keyDesc;

  // schema of the map-reduce 'val' object - this is heterogeneous
  private List<TableDesc> tagToValueDesc;

  private Operator<?> reducer;

  private Integer numReduceTasks;
  private Integer numMapTasks;
  private Integer minSplitSize;

  private boolean needsTagging;
  private boolean hadoopSupportsSplittable;

  private MapredLocalWork mapLocalWork;
  private String inputformat;

  public MapredWork() {
    aliasToPartnInfo = new LinkedHashMap<String, PartitionDesc>();
  }

  public MapredWork(
      final String command,
      final LinkedHashMap<String, ArrayList<String>> pathToAliases,
      final LinkedHashMap<String, PartitionDesc> pathToPartitionInfo,
      final LinkedHashMap<String, Operator<? extends Serializable>> aliasToWork,
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
  public LinkedHashMap<String, Operator<? extends Serializable>> getAliasToWork() {
    return aliasToWork;
  }

  public void setAliasToWork(
      final LinkedHashMap<String, Operator<? extends Serializable>> aliasToWork) {
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

  public Integer getMinSplitSize() {
    return minSplitSize;
  }

  public void setMinSplitSize(Integer minSplitSize) {
    this.minSplitSize = minSplitSize;
  }

  public String getInputformat() {
    return inputformat;
  }

  public void setInputformat(String inputformat) {
    this.inputformat = inputformat;
  }

}
