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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;

/**
 * Map Join operator Descriptor implementation.
 *
 */
@Explain(displayName = "HashTable Sink Operator")
public class HashTableSinkDesc extends JoinDesc implements Serializable {
  private static final long serialVersionUID = 1L;


  // used to handle skew join
  private boolean handleSkewJoin = false;
  private int skewKeyDefinition = -1;
  private Map<Byte, Path> bigKeysDirMap;
  private Map<Byte, Map<Byte, Path>> smallKeysDirMap;
  private Map<Byte, TableDesc> skewKeysValuesTables;

  // alias to key mapping
  private Map<Byte, List<ExprNodeDesc>> exprs;

  // alias to filter mapping
  private Map<Byte, List<ExprNodeDesc>> filters;

  // outerjoin-pos = other-pos:filter-len, other-pos:filter-len, ...
  private int[][] filterMap;

  // used for create joinOutputObjectInspector
  protected List<String> outputColumnNames;

  // key:column output name, value:tag
  private transient Map<String, Byte> reversedExprs;

  // No outer join involved
  protected boolean noOuterJoin;

  protected JoinCondDesc[] conds;

  protected Byte[] tagOrder;
  private TableDesc keyTableDesc;


  private Map<Byte, List<ExprNodeDesc>> keys;
  private TableDesc keyTblDesc;
  private List<TableDesc> valueTblDescs;
  private List<TableDesc> valueTblFilteredDescs;


  private int posBigTable;

  private Map<Byte, List<Integer>> retainList;

  private transient BucketMapJoinContext bucketMapjoinContext;
  private float hashtableMemoryUsage;

  //map join dump file name
  private String dumpFilePrefix;

  public HashTableSinkDesc() {
    bucketMapjoinContext = new BucketMapJoinContext();
  }

  public HashTableSinkDesc(MapJoinDesc clone) {
    this.bigKeysDirMap = clone.getBigKeysDirMap();
    this.conds = clone.getConds();
    this.exprs = new HashMap<Byte, List<ExprNodeDesc>>(clone.getExprs());
    this.handleSkewJoin = clone.getHandleSkewJoin();
    this.keyTableDesc = clone.getKeyTableDesc();
    this.noOuterJoin = clone.getNoOuterJoin();
    this.outputColumnNames = clone.getOutputColumnNames();
    this.reversedExprs = clone.getReversedExprs();
    this.skewKeyDefinition = clone.getSkewKeyDefinition();
    this.skewKeysValuesTables = clone.getSkewKeysValuesTables();
    this.smallKeysDirMap = clone.getSmallKeysDirMap();
    this.tagOrder = clone.getTagOrder();
    this.filters = new HashMap<Byte, List<ExprNodeDesc>>(clone.getFilters());
    this.filterMap = clone.getFilterMap();

    this.keys = new HashMap<Byte, List<ExprNodeDesc>>(clone.getKeys());
    this.keyTblDesc = clone.getKeyTblDesc();
    this.valueTblDescs = clone.getValueTblDescs();
    this.valueTblFilteredDescs = clone.getValueFilteredTblDescs();
    this.posBigTable = clone.getPosBigTable();
    this.retainList = clone.getRetainList();
    this.dumpFilePrefix = clone.getDumpFilePrefix();
    this.bucketMapjoinContext = new BucketMapJoinContext(clone);
    this.hashtableMemoryUsage = clone.getHashTableMemoryUsage();
  }

  public float getHashtableMemoryUsage() {
    return hashtableMemoryUsage;
  }

  public void setHashtableMemoryUsage(float hashtableMemoryUsage) {
    this.hashtableMemoryUsage = hashtableMemoryUsage;
  }

  /**
   * @return the dumpFilePrefix
   */
  public String getDumpFilePrefix() {
    return dumpFilePrefix;
  }

  /**
   * @param dumpFilePrefix
   *          the dumpFilePrefix to set
   */
  public void setDumpFilePrefix(String dumpFilePrefix) {
    this.dumpFilePrefix = dumpFilePrefix;
  }

  public boolean isHandleSkewJoin() {
    return handleSkewJoin;
  }

  @Override
  public void setHandleSkewJoin(boolean handleSkewJoin) {
    this.handleSkewJoin = handleSkewJoin;
  }

  @Override
  public int getSkewKeyDefinition() {
    return skewKeyDefinition;
  }

  @Override
  public void setSkewKeyDefinition(int skewKeyDefinition) {
    this.skewKeyDefinition = skewKeyDefinition;
  }

  @Override
  public Map<Byte, Path> getBigKeysDirMap() {
    return bigKeysDirMap;
  }

  @Override
  public void setBigKeysDirMap(Map<Byte, Path> bigKeysDirMap) {
    this.bigKeysDirMap = bigKeysDirMap;
  }

  @Override
  public Map<Byte, Map<Byte, Path>> getSmallKeysDirMap() {
    return smallKeysDirMap;
  }

  @Override
  public void setSmallKeysDirMap(Map<Byte, Map<Byte, Path>> smallKeysDirMap) {
    this.smallKeysDirMap = smallKeysDirMap;
  }

  @Override
  public Map<Byte, TableDesc> getSkewKeysValuesTables() {
    return skewKeysValuesTables;
  }

  @Override
  public void setSkewKeysValuesTables(Map<Byte, TableDesc> skewKeysValuesTables) {
    this.skewKeysValuesTables = skewKeysValuesTables;
  }

  @Override
  public Map<Byte, List<ExprNodeDesc>> getExprs() {
    return exprs;
  }

  @Override
  public void setExprs(Map<Byte, List<ExprNodeDesc>> exprs) {
    this.exprs = exprs;
  }

  @Override
  public Map<Byte, List<ExprNodeDesc>> getFilters() {
    return filters;
  }


  public List<TableDesc> getValueTblFilteredDescs() {
    return valueTblFilteredDescs;
  }

  public void setValueTblFilteredDescs(List<TableDesc> valueTblFilteredDescs) {
    this.valueTblFilteredDescs = valueTblFilteredDescs;
  }

  @Override
  public void setFilters(Map<Byte, List<ExprNodeDesc>> filters) {
    this.filters = filters;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return outputColumnNames;
  }

  @Override
  public void setOutputColumnNames(List<String> outputColumnNames) {
    this.outputColumnNames = outputColumnNames;
  }

  @Override
  public Map<String, Byte> getReversedExprs() {
    return reversedExprs;
  }

  @Override
  public void setReversedExprs(Map<String, Byte> reversedExprs) {
    this.reversedExprs = reversedExprs;
  }

  @Override
  public boolean isNoOuterJoin() {
    return noOuterJoin;
  }

  @Override
  public void setNoOuterJoin(boolean noOuterJoin) {
    this.noOuterJoin = noOuterJoin;
  }

  @Override
  public JoinCondDesc[] getConds() {
    return conds;
  }

  @Override
  public void setConds(JoinCondDesc[] conds) {
    this.conds = conds;
  }

  @Override
  public Byte[] getTagOrder() {
    return tagOrder;
  }

  @Override
  public void setTagOrder(Byte[] tagOrder) {
    this.tagOrder = tagOrder;
  }

  @Override
  public TableDesc getKeyTableDesc() {
    return keyTableDesc;
  }

  @Override
  public void setKeyTableDesc(TableDesc keyTableDesc) {
    this.keyTableDesc = keyTableDesc;
  }

  @Override
  public int[][] getFilterMap() {
    return filterMap;
  }

  @Override
  public void setFilterMap(int[][] filterMap) {
    this.filterMap = filterMap;
  }

  @Override
  @Explain(displayName = "filter mappings", normalExplain = false)
  public Map<Integer, String> getFilterMapString() {
    return toCompactString(filterMap);
  }

  public Map<Byte, List<Integer>> getRetainList() {
    return retainList;
  }

  public void setRetainList(Map<Byte, List<Integer>> retainList) {
    this.retainList = retainList;
  }

  /**
   * @return the keys in string form
   */
  @Explain(displayName = "keys")
  public Map<Byte, String> getKeysString() {
    Map<Byte, String> keyMap = new LinkedHashMap<Byte, String>();
    for (Map.Entry<Byte, List<ExprNodeDesc>> k: getKeys().entrySet()) {
      keyMap.put(k.getKey(), PlanUtils.getExprListString(k.getValue()));
    }
    return keyMap;
  }

  /**
   * @return the keys
   */
  public Map<Byte, List<ExprNodeDesc>> getKeys() {
    return keys;
  }

  /**
   * @param keys
   *          the keys to set
   */
  public void setKeys(Map<Byte, List<ExprNodeDesc>> keys) {
    this.keys = keys;
  }

  /**
   * @return the position of the big table not in memory
   */
  @Explain(displayName = "Position of Big Table", normalExplain = false)
  public int getPosBigTable() {
    return posBigTable;
  }

  /**
   * @param posBigTable
   *          the position of the big table not in memory
   */
  public void setPosBigTable(int posBigTable) {
    this.posBigTable = posBigTable;
  }

  /**
   * @return the keyTblDesc
   */
  public TableDesc getKeyTblDesc() {
    return keyTblDesc;
  }

  /**
   * @param keyTblDesc
   *          the keyTblDesc to set
   */
  public void setKeyTblDesc(TableDesc keyTblDesc) {
    this.keyTblDesc = keyTblDesc;
  }

  /**
   * @return the valueTblDescs
   */
  public List<TableDesc> getValueTblDescs() {
    return valueTblDescs;
  }

  /**
   * @param valueTblDescs
   *          the valueTblDescs to set
   */
  public void setValueTblDescs(List<TableDesc> valueTblDescs) {
    this.valueTblDescs = valueTblDescs;
  }

  public BucketMapJoinContext getBucketMapjoinContext() {
    return bucketMapjoinContext;
  }

  public void setBucketMapjoinContext(BucketMapJoinContext bucketMapjoinContext) {
    this.bucketMapjoinContext = bucketMapjoinContext;
  }
}
