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


/**
 * Join operator Descriptor implementation.
 *
 */
@Explain(displayName = "Join Operator")
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
  private Map<Byte, String> bigKeysDirMap;
  private Map<Byte, Map<Byte, String>> smallKeysDirMap;
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

  public JoinDesc() {
  }

  public JoinDesc(final Map<Byte, List<ExprNodeDesc>> exprs,
      List<String> outputColumnNames, final boolean noOuterJoin,
      final JoinCondDesc[] conds, final Map<Byte, List<ExprNodeDesc>> filters) {
    this.exprs = exprs;
    this.outputColumnNames = outputColumnNames;
    this.noOuterJoin = noOuterJoin;
    this.conds = conds;
    this.filters = filters;

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
      Map<Byte, String> cloneBigKeysDirMap = new HashMap<Byte, String>();
      cloneBigKeysDirMap.putAll(getBigKeysDirMap());
      ret.setBigKeysDirMap(cloneBigKeysDirMap);
    }
    if (getSmallKeysDirMap() != null) {
      Map<Byte, Map<Byte, String>> cloneSmallKeysDirMap = new HashMap<Byte, Map<Byte,String>> ();
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

  public JoinDesc(final Map<Byte, List<ExprNodeDesc>> exprs,
      List<String> outputColumnNames, final boolean noOuterJoin,
      final JoinCondDesc[] conds) {
    this(exprs, outputColumnNames, noOuterJoin, conds, null);
  }

  public JoinDesc(final Map<Byte, List<ExprNodeDesc>> exprs,
      List<String> outputColumnNames) {
    this(exprs, outputColumnNames, true, null);
  }

  public JoinDesc(final Map<Byte, List<ExprNodeDesc>> exprs,
      List<String> outputColumnNames, final JoinCondDesc[] conds) {
    this(exprs, outputColumnNames, true, conds, null);
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

  @Explain(displayName = "condition expressions")
  public Map<Byte, String> getExprsStringMap() {
    if (getExprs() == null) {
      return null;
    }

    LinkedHashMap<Byte, String> ret = new LinkedHashMap<Byte, String>();

    for (Map.Entry<Byte, List<ExprNodeDesc>> ent : getExprs().entrySet()) {
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      if (ent.getValue() != null) {
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

    return ret;
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
  @Explain(displayName = "filter predicates")
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

  @Explain(displayName = "outputColumnNames")
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

  @Explain(displayName = "condition map")
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

  @Explain(displayName = "handleSkewJoin")
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
  public Map<Byte, String> getBigKeysDirMap() {
    return bigKeysDirMap;
  }

  /**
   * set the mapping from tbl to dir for big keys.
   *
   * @param bigKeysDirMap
   */
  public void setBigKeysDirMap(Map<Byte, String> bigKeysDirMap) {
    this.bigKeysDirMap = bigKeysDirMap;
  }

  /**
   * @return mapping from tbl to dir for small keys
   */
  public Map<Byte, Map<Byte, String>> getSmallKeysDirMap() {
    return smallKeysDirMap;
  }

  /**
   * set the mapping from tbl to dir for small keys.
   *
   * @param smallKeysDirMap
   */
  public void setSmallKeysDirMap(Map<Byte, Map<Byte, String>> smallKeysDirMap) {
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

  @Explain(displayName = "filter mappings", normalExplain = false)
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
}
