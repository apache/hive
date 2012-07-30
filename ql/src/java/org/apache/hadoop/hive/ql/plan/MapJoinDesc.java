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
import java.util.Map.Entry;
import java.util.Set;

/**
 * Map Join operator Descriptor implementation.
 *
 */
@Explain(displayName = "Map Join Operator")
public class MapJoinDesc extends JoinDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  private Map<Byte, List<ExprNodeDesc>> keys;
  private TableDesc keyTblDesc;
  private List<TableDesc> valueTblDescs;
  private List<TableDesc> valueFilteredTblDescs;

  private int posBigTable;

  private Map<Byte, List<Integer>> retainList;

  private transient String bigTableAlias;

  private Map<String, Map<String, List<String>>> aliasBucketFileNameMapping;
  private Map<String, Integer> bigTableBucketNumMapping;
  private Map<String, List<String>> bigTablePartSpecToFileMapping;

  //map join dump file name
  private String dumpFilePrefix;

  public MapJoinDesc() {
    bigTableBucketNumMapping = new LinkedHashMap<String, Integer>();
  }

  public MapJoinDesc(MapJoinDesc clone) {
    super(clone);
    this.keys = clone.keys;
    this.keyTblDesc = clone.keyTblDesc;
    this.valueTblDescs = clone.valueTblDescs;
    this.posBigTable = clone.posBigTable;
    this.retainList = clone.retainList;
    this.bigTableAlias = clone.bigTableAlias;
    this.aliasBucketFileNameMapping = clone.aliasBucketFileNameMapping;
    this.bigTableBucketNumMapping = clone.bigTableBucketNumMapping;
    this.bigTablePartSpecToFileMapping = clone.bigTablePartSpecToFileMapping;
    this.dumpFilePrefix = clone.dumpFilePrefix;
  }

  public MapJoinDesc(final Map<Byte, List<ExprNodeDesc>> keys,
      final TableDesc keyTblDesc, final Map<Byte, List<ExprNodeDesc>> values,
      final List<TableDesc> valueTblDescs,final List<TableDesc> valueFilteredTblDescs,  List<String> outputColumnNames,
      final int posBigTable, final JoinCondDesc[] conds,
      final Map<Byte, List<ExprNodeDesc>> filters, boolean noOuterJoin, String dumpFilePrefix) {
    super(values, outputColumnNames, noOuterJoin, conds, filters);
    this.keys = keys;
    this.keyTblDesc = keyTblDesc;
    this.valueTblDescs = valueTblDescs;
    this.valueFilteredTblDescs = valueFilteredTblDescs;
    this.posBigTable = posBigTable;
    this.bigTableBucketNumMapping = new LinkedHashMap<String, Integer>();
    this.dumpFilePrefix = dumpFilePrefix;
    initRetainExprList();
  }

  private void initRetainExprList() {
    retainList = new HashMap<Byte, List<Integer>>();
    Set<Entry<Byte, List<ExprNodeDesc>>> set = super.getExprs().entrySet();
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

  public Map<Byte, List<Integer>> getRetainList() {
    return retainList;
  }

  public void setRetainList(Map<Byte, List<Integer>> retainList) {
    this.retainList = retainList;
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

  public List<TableDesc> getValueFilteredTblDescs() {
    return valueFilteredTblDescs;
  }

  public void setValueFilteredTblDescs(List<TableDesc> valueFilteredTblDescs) {
    this.valueFilteredTblDescs = valueFilteredTblDescs;
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

  public Map<String, Map<String, List<String>>> getAliasBucketFileNameMapping() {
    return aliasBucketFileNameMapping;
  }

  public void setAliasBucketFileNameMapping(
      Map<String, Map<String, List<String>>> aliasBucketFileNameMapping) {
    this.aliasBucketFileNameMapping = aliasBucketFileNameMapping;
  }

  public Map<String, Integer> getBigTableBucketNumMapping() {
    return bigTableBucketNumMapping;
  }

  public void setBigTableBucketNumMapping(Map<String, Integer> bigTableBucketNumMapping) {
    this.bigTableBucketNumMapping = bigTableBucketNumMapping;
  }

  public Map<String, List<String>> getBigTablePartSpecToFileMapping() {
    return bigTablePartSpecToFileMapping;
  }

  public void setBigTablePartSpecToFileMapping(Map<String, List<String>> partToFileMapping) {
    this.bigTablePartSpecToFileMapping = partToFileMapping;
  }
}
