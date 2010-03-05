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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Join operator Descriptor implementation.
 * 
 */
@Explain(displayName = "Join Operator")
public class JoinDesc implements Serializable {
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

  // used for create joinOutputObjectInspector
  protected java.util.ArrayList<java.lang.String> outputColumnNames;

  // key:column output name, value:tag
  private transient Map<String, Byte> reversedExprs;

  // No outer join involved
  protected boolean noOuterJoin;

  protected JoinCondDesc[] conds;

  protected Byte[] tagOrder;
  private TableDesc keyTableDesc;

  public JoinDesc() {
  }

  public JoinDesc(final Map<Byte, List<ExprNodeDesc>> exprs,
      ArrayList<String> outputColumnNames, final boolean noOuterJoin,
      final JoinCondDesc[] conds) {
    this.exprs = exprs;
    this.outputColumnNames = outputColumnNames;
    this.noOuterJoin = noOuterJoin;
    this.conds = conds;

    tagOrder = new Byte[exprs.size()];
    for (int i = 0; i < tagOrder.length; i++) {
      tagOrder[i] = (byte) i;
    }
  }

  public JoinDesc(final Map<Byte, List<ExprNodeDesc>> exprs,
      ArrayList<String> outputColumnNames) {
    this(exprs, outputColumnNames, true, null);
  }

  public JoinDesc(final Map<Byte, List<ExprNodeDesc>> exprs,
      ArrayList<String> outputColumnNames, final JoinCondDesc[] conds) {
    this(exprs, outputColumnNames, false, conds);
  }

  public JoinDesc(JoinDesc clone) {
    this.bigKeysDirMap = clone.bigKeysDirMap;
    this.conds = clone.conds;
    this.exprs = clone.exprs;
    this.handleSkewJoin = clone.handleSkewJoin;
    this.keyTableDesc = clone.keyTableDesc;
    this.noOuterJoin = clone.noOuterJoin;
    this.outputColumnNames = clone.outputColumnNames;
    this.reversedExprs = clone.reversedExprs;
    this.skewKeyDefinition = clone.skewKeyDefinition;
    this.skewKeysValuesTables = clone.skewKeysValuesTables;
    this.smallKeysDirMap = clone.smallKeysDirMap;
    this.tagOrder = clone.tagOrder;
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

  @Explain(displayName = "outputColumnNames")
  public java.util.ArrayList<java.lang.String> getOutputColumnNames() {
    return outputColumnNames;
  }

  public void setOutputColumnNames(
      java.util.ArrayList<java.lang.String> outputColumnNames) {
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
   * @param bigKeysDirMap
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
   * @param skewKeysValuesTable
   *          set the table desc for storing skew keys and their corresponding
   *          value;
   */
  public void setSkewKeysValuesTables(Map<Byte, TableDesc> skewKeysValuesTables) {
    this.skewKeysValuesTables = skewKeysValuesTables;
  }

  public boolean isNoOuterJoin() {
    for (org.apache.hadoop.hive.ql.plan.JoinCondDesc cond : conds) {
      if (cond.getType() == JoinDesc.FULL_OUTER_JOIN
          || (cond.getType() == JoinDesc.LEFT_OUTER_JOIN)
          || cond.getType() == JoinDesc.RIGHT_OUTER_JOIN) {
        return false;
      }
    }
    return true;
  }

  public void setKeyTableDesc(TableDesc keyTblDesc) {
    keyTableDesc = keyTblDesc;
  }

  public TableDesc getKeyTableDesc() {
    return keyTableDesc;
  }
}
