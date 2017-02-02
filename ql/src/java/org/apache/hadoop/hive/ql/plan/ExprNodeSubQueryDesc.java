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

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.calcite.rel.RelNode;

/**
 * This encapsulate subquery expression which consists of
 *  Relnode for subquery.
 *  type (IN, EXISTS )
 *  LHS operand
 */
public class ExprNodeSubQueryDesc extends ExprNodeDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  public static enum SubqueryType{
    IN,
    EXISTS,
    SCALAR
  };

  /**
   * RexNode corresponding to subquery.
   */
  private RelNode rexSubQuery;
  private ExprNodeDesc subQueryLhs;
  private SubqueryType type;

  public ExprNodeSubQueryDesc(TypeInfo typeInfo, RelNode subQuery, SubqueryType type) {
    super(typeInfo);
    this.rexSubQuery = subQuery;
    this.subQueryLhs = null;
    this.type = type;
  }
  public ExprNodeSubQueryDesc(TypeInfo typeInfo, RelNode subQuery,
                              SubqueryType type, ExprNodeDesc lhs) {
    super(typeInfo);
    this.rexSubQuery = subQuery;
    this.subQueryLhs = lhs;
    this.type = type;

  }

  public SubqueryType getType() {
    return type;
  }

  public ExprNodeDesc getSubQueryLhs() {
    return subQueryLhs;
  }

  public RelNode getRexSubQuery() {
    return rexSubQuery;
  }

  @Override
  public ExprNodeDesc clone() {
    return new ExprNodeSubQueryDesc(typeInfo, rexSubQuery, type, subQueryLhs);
  }

  @Override
  public boolean isSame(Object o) {
    if (!(o instanceof ExprNodeSubQueryDesc)) {
      return false;
    }
    ExprNodeSubQueryDesc dest = (ExprNodeSubQueryDesc) o;
    if (subQueryLhs != null && dest.getSubQueryLhs() != null) {
      if (!subQueryLhs.equals(dest.getSubQueryLhs())) {
        return false;
      }
    }
    if (!typeInfo.equals(dest.getTypeInfo())) {
      return false;
    }
    if (!rexSubQuery.equals(dest.getRexSubQuery())) {
      return false;
    }
    if(type != dest.getType()) {
      return false;
    }
    return true;
  }
}
