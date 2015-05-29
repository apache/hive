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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.plan.Explain.Level;


/**
 * Join operator Descriptor implementation.
 *
 */
@Explain(displayName = "Join Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class JoinDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;
  public static final int INNER_JOIN = 0;
  public static final int LEFT_OUTER_JOIN = 1;
  public static final int RIGHT_OUTER_JOIN = 2;
  public static final int FULL_OUTER_JOIN = 3;
  public static final int UNIQUE_JOIN = 4;
  public static final int LEFT_SEMI_JOIN = 5;

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

  // pos of outer join alias=<pos of other alias:num of filters on outer join alias>xn
  // for example,
  // a left outer join b on a.k=b.k AND a.k>5 full outer join c on a.k=c.k AND a.k>10 AND c.k>20
  //
  // That means on a(pos=0), there are overlapped filters associated with b(pos=1) and c(pos=2).
  // (a)b has one filter on a (a.k>5) and (a)c also has one filter on a (a.k>10),
  // making filter map for a as 0=1:1:2:1.
  // C also has one outer join filter associated with A(c.k>20), which is making 2=0:1
  private int[][] filterMap;

  // key index to nullsafe join flag
  private boolean[] nullsafes;

  // used for create joinOutputObjectInspector
  protected List<String> outputColumnNames;

  // key:column output name, value:tag
  private transient Map<String, Byte> reversedExprs;

  // No outer join involved
  protected boolean noOuterJoin;

  protected JoinCondDesc[] conds;

  protected Byte[] tagOrder;
  private TableDesc keyTableDesc;

  // this operator cannot be converted to mapjoin cause output is expected to be sorted on join key
  // it's resulted from RS-dedup optimization, which removes following RS under some condition
  private boolean fixedAsSorted;

  // used only for explain.
  private transient ExprNodeDesc [][] joinKeys;

  // Data structures coming originally from QBJoinTree
  private transient String leftAlias;
  private transient String[] leftAliases;
  private transient String[] rightAliases;
  private transient String[] baseSrc;
  private transient String id;
  private transient boolean mapSideJoin;
  private transient List<String> mapAliases; //map-side join aliases
  private transient Map<String, Operator<? extends OperatorDesc>> aliasToOpInfo;
  private transient boolean leftInputJoin;
  private transient List<String> streamAliases;

  public JoinDesc() {
  }

  public JoinDesc(final Map<Byte, List<ExprNodeDesc>> exprs,
          List<String> outputColumnNames, final boolean noOuterJoin,
          final JoinCondDesc[] conds, ExprNodeDesc[][] joinKeys) {
    this (exprs, outputColumnNames, noOuterJoin, conds,
            new HashMap<Byte, List<ExprNodeDesc>>(), joinKeys);
  }

  public JoinDesc(final Map<Byte, List<ExprNodeDesc>> exprs,
      List<String> outputColumnNames, final boolean noOuterJoin,
      final JoinCondDesc[] conds, final Map<Byte, List<ExprNodeDesc>> filters,
      ExprNodeDesc[][] joinKeys) {
    this.exprs = exprs;
    this.outputColumnNames = outputColumnNames;
    this.noOuterJoin = noOuterJoin;
    this.conds = conds;
    this.filters = filters;
    this.joinKeys = joinKeys;

    resetOrder();
  }

  // called by late-MapJoin processor (hive.auto.convert.join=true for example)
  public void resetOrder() {
    tagOrder = new Byte[exprs.size()];
    for (int i = 0; i < tagOrder.length; i++) {
      tagOrder[i] = (byte) i;
    }
  }

  @Override
  public Object clone() {
    JoinDesc ret = new JoinDesc();
    Map<Byte,List<ExprNodeDesc>> cloneExprs = new HashMap<Byte,List<ExprNodeDesc>>();
    cloneExprs.putAll(getExprs());
    ret.setExprs(cloneExprs);
    Map<Byte,List<ExprNodeDesc>> cloneFilters = new HashMap<Byte,List<ExprNodeDesc>>();
    cloneFilters.putAll(getFilters());
    ret.setFilters(cloneFilters);
    ret.setConds(getConds().clone());
    ret.setNoOuterJoin(getNoOuterJoin());
    ret.setNullSafes(getNullSafes());
    ret.setHandleSkewJoin(handleSkewJoin);
    ret.setSkewKeyDefinition(getSkewKeyDefinition());
    ret.setTagOrder(getTagOrder().clone());
    if (getKeyTableDesc() != null) {
      ret.setKeyTableDesc((TableDesc) getKeyTableDesc().clone());
    }

    if (getBigKeysDirMap() != null) {
      Map<Byte, Path> cloneBigKeysDirMap = new HashMap<Byte, Path>();
      cloneBigKeysDirMap.putAll(getBigKeysDirMap());
      ret.setBigKeysDirMap(cloneBigKeysDirMap);
    }
    if (getSmallKeysDirMap() != null) {
      Map<Byte, Map<Byte, Path>> cloneSmallKeysDirMap = new HashMap<Byte, Map<Byte,Path>> ();
      cloneSmallKeysDirMap.putAll(getSmallKeysDirMap());
      ret.setSmallKeysDirMap(cloneSmallKeysDirMap);
    }
    if (getSkewKeysValuesTables() != null) {
      Map<Byte, TableDesc> cloneSkewKeysValuesTables = new HashMap<Byte, TableDesc>();
      cloneSkewKeysValuesTables.putAll(getSkewKeysValuesTables());
      ret.setSkewKeysValuesTables(cloneSkewKeysValuesTables);
    }
    if (getOutputColumnNames() != null) {
      List<String> cloneOutputColumnNames = new ArrayList<String>();
      cloneOutputColumnNames.addAll(getOutputColumnNames());
      ret.setOutputColumnNames(cloneOutputColumnNames);
    }
    if (getReversedExprs() != null) {
      Map<String, Byte> cloneReversedExprs = new HashMap<String, Byte>();
      cloneReversedExprs.putAll(getReversedExprs());
      ret.setReversedExprs(cloneReversedExprs);
    }
    return ret;
  }

  public JoinDesc(JoinDesc clone) {
    this.bigKeysDirMap = clone.bigKeysDirMap;
    this.conds = clone.conds;
    this.exprs = clone.exprs;
    this.nullsafes = clone.nullsafes;
    this.handleSkewJoin = clone.handleSkewJoin;
    this.keyTableDesc = clone.keyTableDesc;
    this.noOuterJoin = clone.noOuterJoin;
    this.outputColumnNames = clone.outputColumnNames;
    this.reversedExprs = clone.reversedExprs;
    this.skewKeyDefinition = clone.skewKeyDefinition;
    this.skewKeysValuesTables = clone.skewKeysValuesTables;
    this.smallKeysDirMap = clone.smallKeysDirMap;
    this.tagOrder = clone.tagOrder;
    this.filters = clone.filters;
    this.filterMap = clone.filterMap;
    this.statistics = clone.statistics;
  }

  public Map<Byte, List<ExprNodeDesc>> getExprs() {
    return exprs;
  }

  public Map<String, Byte> getReversedExprs() {
    return reversedExprs;
  }

  public void setReversedExprs(Map<String, Byte> reversedExprs) {
    this.reversedExprs = reversedExprs;
  }

  /**
   * @return the keys in string form
   */
  @Explain(displayName = "keys", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Map<Byte, String> getKeysString() {
    Map<Byte, String> keyMap = new LinkedHashMap<Byte, String>();
    for (byte i = 0; i < joinKeys.length; i++) {
      keyMap.put(i, PlanUtils.getExprListString(Arrays.asList(joinKeys[i])));
    }
    return keyMap;
  }

  public void setExprs(final Map<Byte, List<ExprNodeDesc>> exprs) {
    this.exprs = exprs;
  }

  /**
   * Get the string representation of filters.
   *
   * Returns null if they are no filters.
   *
   * @return Map from alias to filters on the alias.
   */
  @Explain(displayName = "filter predicates", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Map<Byte, String> getFiltersStringMap() {
    if (getFilters() == null || getFilters().size() == 0) {
      return null;
    }

    LinkedHashMap<Byte, String> ret = new LinkedHashMap<Byte, String>();
    boolean filtersPresent = false;

    for (Map.Entry<Byte, List<ExprNodeDesc>> ent : getFilters().entrySet()) {
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      if (ent.getValue() != null) {
        if (ent.getValue().size() != 0) {
          filtersPresent = true;
        }
        for (ExprNodeDesc expr : ent.getValue()) {
          if (!first) {
            sb.append(" ");
          }

          first = false;
          sb.append("{");
          sb.append(expr.getExprString());
          sb.append("}");
        }
      }
      ret.put(ent.getKey(), sb.toString());
    }

    if (filtersPresent) {
      return ret;
    } else {
      return null;
    }
  }


  public Map<Byte, List<ExprNodeDesc>> getFilters() {
    return filters;
  }

  public void setFilters(Map<Byte, List<ExprNodeDesc>> filters) {
    this.filters = filters;
  }

  @Explain(displayName = "outputColumnNames", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public List<String> getOutputColumnNames() {
    return outputColumnNames;
  }

  public void setOutputColumnNames(
      List<String> outputColumnNames) {
    this.outputColumnNames = outputColumnNames;
  }

  public boolean getNoOuterJoin() {
    return noOuterJoin;
  }

  public void setNoOuterJoin(final boolean noOuterJoin) {
    this.noOuterJoin = noOuterJoin;
  }

  @Explain(displayName = "condition map", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public List<JoinCondDesc> getCondsList() {
    if (conds == null) {
      return null;
    }

    ArrayList<JoinCondDesc> l = new ArrayList<JoinCondDesc>();
    for (JoinCondDesc cond : conds) {
      l.add(cond);
    }

    return l;
  }

  public JoinCondDesc[] getConds() {
    return conds;
  }

  public void setConds(final JoinCondDesc[] conds) {
    this.conds = conds;
  }

  /**
   * The order in which tables should be processed when joining.
   *
   * @return Array of tags
   */
  public Byte[] getTagOrder() {
    return tagOrder;
  }

  /**
   * The order in which tables should be processed when joining.
   *
   * @param tagOrder
   *          Array of tags
   */
  public void setTagOrder(Byte[] tagOrder) {
    this.tagOrder = tagOrder;
  }

  @Explain(displayName = "handleSkewJoin", displayOnlyOnTrue = true)
  public boolean getHandleSkewJoin() {
    return handleSkewJoin;
  }

  /**
   * set to handle skew join in this join op.
   *
   * @param handleSkewJoin
   */
  public void setHandleSkewJoin(boolean handleSkewJoin) {
    this.handleSkewJoin = handleSkewJoin;
  }

  /**
   * @return mapping from tbl to dir for big keys.
   */
  public Map<Byte, Path> getBigKeysDirMap() {
    return bigKeysDirMap;
  }

  /**
   * set the mapping from tbl to dir for big keys.
   *
   * @param bigKeysDirMap
   */
  public void setBigKeysDirMap(Map<Byte, Path> bigKeysDirMap) {
    this.bigKeysDirMap = bigKeysDirMap;
  }

  /**
   * @return mapping from tbl to dir for small keys
   */
  public Map<Byte, Map<Byte, Path>> getSmallKeysDirMap() {
    return smallKeysDirMap;
  }

  /**
   * set the mapping from tbl to dir for small keys.
   *
   * @param smallKeysDirMap
   */
  public void setSmallKeysDirMap(Map<Byte, Map<Byte, Path>> smallKeysDirMap) {
    this.smallKeysDirMap = smallKeysDirMap;
  }

  /**
   * @return skew key definition. If we see a key's associated entries' number
   *         is bigger than this, we will define this key as a skew key.
   */
  public int getSkewKeyDefinition() {
    return skewKeyDefinition;
  }

  /**
   * set skew key definition.
   *
   * @param skewKeyDefinition
   */
  public void setSkewKeyDefinition(int skewKeyDefinition) {
    this.skewKeyDefinition = skewKeyDefinition;
  }

  /**
   * @return the table desc for storing skew keys and their corresponding value;
   */
  public Map<Byte, TableDesc> getSkewKeysValuesTables() {
    return skewKeysValuesTables;
  }

  /**
   * @param skewKeysValuesTables
   *          set the table desc for storing skew keys and their corresponding
   *          value;
   */
  public void setSkewKeysValuesTables(Map<Byte, TableDesc> skewKeysValuesTables) {
    this.skewKeysValuesTables = skewKeysValuesTables;
  }

  public boolean isNoOuterJoin() {
    return noOuterJoin;
  }

  public void setKeyTableDesc(TableDesc keyTblDesc) {
    keyTableDesc = keyTblDesc;
  }

  public TableDesc getKeyTableDesc() {
    return keyTableDesc;
  }

  public boolean[] getNullSafes() {
    return nullsafes;
  }

  public void setNullSafes(boolean[] nullSafes) {
    this.nullsafes = nullSafes;
  }

  @Explain(displayName = "nullSafes")
  public String getNullSafeString() {
    if (nullsafes == null) {
      return null;
    }
    boolean hasNS = false;
    for (boolean ns : nullsafes) {
      hasNS |= ns;
    }
    return hasNS ? Arrays.toString(nullsafes) : null;
  }

  public int[][] getFilterMap() {
    return filterMap;
  }

  public void setFilterMap(int[][] filterMap) {
    this.filterMap = filterMap;
  }

  @Explain(displayName = "filter mappings", explainLevels = { Level.EXTENDED })
  public Map<Integer, String> getFilterMapString() {
    return toCompactString(filterMap);
  }

  protected Map<Integer, String> toCompactString(int[][] filterMap) {
    if (filterMap == null) {
      return null;
    }
    filterMap = compactFilter(filterMap);
    Map<Integer, String> result = new LinkedHashMap<Integer, String>();
    for (int i = 0 ; i < filterMap.length; i++) {
      if (filterMap[i] == null) {
        continue;
      }
      result.put(i, Arrays.toString(filterMap[i]));
    }
    return result.isEmpty() ? null : result;
  }

  // remove filterMap for outer alias if filter is not exist on that
  private int[][] compactFilter(int[][] filterMap) {
    if (filterMap == null) {
      return null;
    }
    for (int i = 0; i < filterMap.length; i++) {
      if (filterMap[i] != null) {
        boolean noFilter = true;
        // join positions for even index, filter lengths for odd index
        for (int j = 1; j < filterMap[i].length; j += 2) {
          if (filterMap[i][j] > 0) {
            noFilter = false;
            break;
          }
        }
        if (noFilter) {
          filterMap[i] = null;
        }
      }
    }
    for (int[] mapping : filterMap) {
      if (mapping != null) {
        return filterMap;
      }
    }
    return null;
  }

  public int getTagLength() {
    int tagLength = -1;
    for (byte tag : getExprs().keySet()) {
      tagLength = Math.max(tagLength, tag + 1);
    }
    return tagLength;
  }

  @SuppressWarnings("unchecked")
  public <T> T[] convertToArray(Map<Byte, T> source, Class<T> compType) {
    T[] result = (T[]) Array.newInstance(compType, getTagLength());
    for (Map.Entry<Byte, T> entry : source.entrySet()) {
      result[entry.getKey()] = entry.getValue();
    }
    return result;
  }

  public boolean isFixedAsSorted() {
    return fixedAsSorted;
  }

  public void setFixedAsSorted(boolean fixedAsSorted) {
    this.fixedAsSorted = fixedAsSorted;
  }

  public String[] getLeftAliases() {
    return leftAliases;
  }

  public String[] getBaseSrc() {
    return baseSrc;
  }

  public void setBaseSrc(String[] baseSrc) {
    this.baseSrc = baseSrc;
  }

  public String getId() {
    return id;
  }

  public List<String> getMapAliases() {
    return mapAliases;
  }

  public Map<String, Operator<? extends OperatorDesc>> getAliasToOpInfo() {
    return aliasToOpInfo;
  }

  public void setAliasToOpInfo(Map<String, Operator<? extends OperatorDesc>> aliasToOpInfo) {
    this.aliasToOpInfo = aliasToOpInfo;
  }

  public boolean isLeftInputJoin() {
    return leftInputJoin;
  }

  public String getLeftAlias() {
    return leftAlias;
  }

  public void setLeftAlias(String leftAlias) {
    this.leftAlias = leftAlias;
  }

  public String[] getRightAliases() {
    return rightAliases;
  }

  public void setRightAliases(String[] rightAliases) {
    this.rightAliases = rightAliases;
  }

  public List<String> getStreamAliases() {
    return streamAliases;
  }

  public boolean isMapSideJoin() {
    return mapSideJoin;
  }

  public void setQBJoinTreeProps(JoinDesc joinDesc) {
    leftAlias = joinDesc.leftAlias;
    leftAliases = joinDesc.leftAliases;
    rightAliases = joinDesc.rightAliases;
    baseSrc = joinDesc.baseSrc;
    id = joinDesc.id;
    mapSideJoin = joinDesc.mapSideJoin;
    mapAliases = joinDesc.mapAliases;
    aliasToOpInfo = joinDesc.aliasToOpInfo;
    leftInputJoin = joinDesc.leftInputJoin;
    streamAliases = joinDesc.streamAliases;
  }

  public void setQBJoinTreeProps(QBJoinTree joinTree) {
    leftAlias = joinTree.getLeftAlias();
    leftAliases = joinTree.getLeftAliases();
    rightAliases = joinTree.getRightAliases();
    baseSrc = joinTree.getBaseSrc();
    id = joinTree.getId();
    mapSideJoin = joinTree.isMapSideJoin();
    mapAliases = joinTree.getMapAliases();
    aliasToOpInfo = joinTree.getAliasToOpInfo();
    leftInputJoin = joinTree.getJoinSrc() != null;
    streamAliases = joinTree.getStreamAliases();
  }

  public void cloneQBJoinTreeProps(JoinDesc joinDesc) {
    leftAlias = joinDesc.leftAlias;
    leftAliases = joinDesc.leftAliases == null ? null : joinDesc.leftAliases.clone();
    rightAliases = joinDesc.rightAliases == null ? null : joinDesc.rightAliases.clone();
    baseSrc = joinDesc.baseSrc == null ? null : joinDesc.baseSrc.clone();
    id = joinDesc.id;
    mapSideJoin = joinDesc.mapSideJoin;
    mapAliases = joinDesc.mapAliases == null ? null : new ArrayList<String>(joinDesc.mapAliases);
    aliasToOpInfo = new HashMap<String, Operator<? extends OperatorDesc>>(joinDesc.aliasToOpInfo);
    leftInputJoin = joinDesc.leftInputJoin;
    streamAliases = joinDesc.streamAliases == null ? null : new ArrayList<String>(joinDesc.streamAliases);
  }

}
