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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

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
  private Map<Byte, String> bigKeysDirMap;
  private Map<Byte, Map<Byte, String>> smallKeysDirMap;
  private Map<Byte, TableDesc> skewKeysValuesTables;

  // alias to key mapping
  private Map<Byte, List<ExprNodeDesc>> exprs;

  // alias to filter mapping
  private Map<Byte, List<ExprNodeDesc>> filters;

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

  private transient String bigTableAlias;

  private LinkedHashMap<String, LinkedHashMap<String, ArrayList<String>>> aliasBucketFileNameMapping;
  private LinkedHashMap<String, Integer> bucketFileNameMapping;
  private float hashtableMemoryUsage;

  public HashTableSinkDesc() {
    bucketFileNameMapping = new LinkedHashMap<String, Integer>();
  }

  public HashTableSinkDesc(MapJoinDesc clone) {
    this.bigKeysDirMap = clone.getBigKeysDirMap();
    this.conds = clone.getConds();
    this.exprs= clone.getExprs();
    this.handleSkewJoin = clone.getHandleSkewJoin();
    this.keyTableDesc = clone.getKeyTableDesc();
    this.noOuterJoin = clone.getNoOuterJoin();
    this.outputColumnNames = clone.getOutputColumnNames();
    this.reversedExprs = clone.getReversedExprs();
    this.skewKeyDefinition = clone.getSkewKeyDefinition();
    this.skewKeysValuesTables = clone.getSkewKeysValuesTables();
    this.smallKeysDirMap = clone.getSmallKeysDirMap();
    this.tagOrder = clone.getTagOrder();
    this.filters = clone.getFilters();

    this.keys = clone.getKeys();
    this.keyTblDesc = clone.getKeyTblDesc();
    this.valueTblDescs = clone.getValueTblDescs();
    this.valueTblFilteredDescs = clone.getValueFilteredTblDescs();
    this.posBigTable = clone.getPosBigTable();
    this.retainList = clone.getRetainList();
    this.bigTableAlias = clone.getBigTableAlias();
    this.aliasBucketFileNameMapping = clone.getAliasBucketFileNameMapping();
    this.bucketFileNameMapping = clone.getBucketFileNameMapping();
  }


  private void initRetainExprList() {
    retainList = new HashMap<Byte, List<Integer>>();
    Set<Entry<Byte, List<ExprNodeDesc>>> set = exprs.entrySet();
    Iterator<Entry<Byte, List<ExprNodeDesc>>> setIter = set.iterator();
    while (setIter.hasNext()) {
      Entry<Byte, List<ExprNodeDesc>> current = setIter.next();
      List<Integer> list = new ArrayList<Integer>();
      for (int i = 0; i < current.getValue().size(); i++) {
        list.add(i);
      }
      retainList.put(current.getKey(), list);
    }
  }

  public float getHashtableMemoryUsage() {
    return hashtableMemoryUsage;
  }

  public void setHashtableMemoryUsage(float hashtableMemoryUsage) {
    this.hashtableMemoryUsage = hashtableMemoryUsage;
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
  public Map<Byte, String> getBigKeysDirMap() {
    return bigKeysDirMap;
  }

  @Override
  public void setBigKeysDirMap(Map<Byte, String> bigKeysDirMap) {
    this.bigKeysDirMap = bigKeysDirMap;
  }

  @Override
  public Map<Byte, Map<Byte, String>> getSmallKeysDirMap() {
    return smallKeysDirMap;
  }

  @Override
  public void setSmallKeysDirMap(Map<Byte, Map<Byte, String>> smallKeysDirMap) {
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


  public Map<Byte, List<Integer>> getRetainList() {
    return retainList;
  }

  public void setRetainList(Map<Byte, List<Integer>> retainList) {
    this.retainList = retainList;
  }

  /**
   * @return the keys
   */
  @Explain(displayName = "keys")
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
  @Explain(displayName = "Position of Big Table")
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

  /**
   * @return bigTableAlias
   */
  public String getBigTableAlias() {
    return bigTableAlias;
  }

  /**
   * @param bigTableAlias
   */
  public void setBigTableAlias(String bigTableAlias) {
    this.bigTableAlias = bigTableAlias;
  }

  public LinkedHashMap<String, LinkedHashMap<String, ArrayList<String>>> getAliasBucketFileNameMapping() {
    return aliasBucketFileNameMapping;
  }

  public void setAliasBucketFileNameMapping(
      LinkedHashMap<String, LinkedHashMap<String, ArrayList<String>>> aliasBucketFileNameMapping) {
    this.aliasBucketFileNameMapping = aliasBucketFileNameMapping;
  }

  public LinkedHashMap<String, Integer> getBucketFileNameMapping() {
    return bucketFileNameMapping;
  }

  public void setBucketFileNameMapping(LinkedHashMap<String, Integer> bucketFileNameMapping) {
    this.bucketFileNameMapping = bucketFileNameMapping;
  }
}
