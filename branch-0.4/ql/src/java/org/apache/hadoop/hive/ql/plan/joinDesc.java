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

import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

/**
 * Join operator Descriptor implementation.
 * 
 */
@explain(displayName="Join Operator")
public class joinDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  public static final int INNER_JOIN = 0;
  public static final int LEFT_OUTER_JOIN = 1;
  public static final int RIGHT_OUTER_JOIN = 2;
  public static final int FULL_OUTER_JOIN = 3;

  // alias to key mapping
  private Map<Byte, List<exprNodeDesc>> exprs;
  
  //used for create joinOutputObjectInspector
  protected java.util.ArrayList<java.lang.String> outputColumnNames;
  
  // key:column output name, value:tag
  transient private Map<String, Byte> reversedExprs;
  
  // No outer join involved
  protected boolean noOuterJoin;

  protected joinCond[] conds;
  
  public joinDesc() { }
  
  public joinDesc(final Map<Byte, List<exprNodeDesc>> exprs, ArrayList<String> outputColumnNames, final boolean noOuterJoin, final joinCond[] conds) {
    this.exprs = exprs;
    this.outputColumnNames = outputColumnNames;
    this.noOuterJoin = noOuterJoin;
    this.conds = conds;
  }
  
  public joinDesc(final Map<Byte, List<exprNodeDesc>> exprs, ArrayList<String> outputColumnNames) {
    this(exprs, outputColumnNames, true, null);
  }

  public joinDesc(final Map<Byte, List<exprNodeDesc>> exprs, ArrayList<String> outputColumnNames, final joinCond[] conds) {
    this(exprs, outputColumnNames, false, conds);
  }
  
  public Map<Byte, List<exprNodeDesc>> getExprs() {
    return this.exprs;
  }
  
  public Map<String, Byte> getReversedExprs() {
    return reversedExprs;
  }

  public void setReversedExprs(Map<String, Byte> reversed_Exprs) {
    this.reversedExprs = reversed_Exprs;
  }
  
  @explain(displayName="condition expressions")
  public Map<Byte, String> getExprsStringMap() {
    if (getExprs() == null) {
      return null;
    }
    
    LinkedHashMap<Byte, String> ret = new LinkedHashMap<Byte, String>();
    
    for(Map.Entry<Byte, List<exprNodeDesc>> ent: getExprs().entrySet()) {
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      if (ent.getValue() != null) {
        for(exprNodeDesc expr: ent.getValue()) {
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
  
  public void setExprs(final Map<Byte, List<exprNodeDesc>> exprs) {
    this.exprs = exprs;
  }
  
  @explain(displayName="outputColumnNames")
  public java.util.ArrayList<java.lang.String> getOutputColumnNames() {
    return outputColumnNames;
  }

  public void setOutputColumnNames(
      java.util.ArrayList<java.lang.String> outputColumnNames) {
    this.outputColumnNames = outputColumnNames;
  }

  public boolean getNoOuterJoin() {
    return this.noOuterJoin;
  }

  public void setNoOuterJoin(final boolean noOuterJoin) {
    this.noOuterJoin = noOuterJoin;
  }

  @explain(displayName="condition map")
  public List<joinCond> getCondsList() {
    if (conds == null) {
      return null;
    }

    ArrayList<joinCond> l = new ArrayList<joinCond>();
    for(joinCond cond: conds) {
      l.add(cond);
    }

    return l;
  }

  public joinCond[] getConds() {
    return this.conds;
  }

  public void setConds(final joinCond[] conds) {
    this.conds = conds;
  }

}
