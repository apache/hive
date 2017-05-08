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

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * A constant expression with default value and data type. The value is different
 * from any value of that data type. Used to represent the default partition in
 * the expression of x =/!= __HIVE_DEFAULT_PARTITION__
 */
public class ExprNodeConstantDefaultDesc extends ExprNodeDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  private final Object value;     // The internal value for the default

  public ExprNodeConstantDefaultDesc() {
    value = null;
  }

  public ExprNodeConstantDefaultDesc(TypeInfo typeInfo, Object value) {
    super(typeInfo);
    this.value = value;
  }

  @Override
  public String toString() {
    return "Const " + typeInfo.toString() + " default";
  }

  @Override
  public String getExprString() {
    return value == null ? "null" : value.toString();
  }

  @Override
  public boolean isSame(Object o) {
    if (!(o instanceof ExprNodeConstantDefaultDesc)) {
      return false;
    }
    ExprNodeConstantDefaultDesc dest = (ExprNodeConstantDefaultDesc) o;
    if (!typeInfo.equals(dest.getTypeInfo())) {
      return false;
    }
    if (value == null) {
      if (dest.value != null) {
        return false;
      }
    } else if (!value.equals(dest.value)) {
      return false;
    }

    return true;
  }

  @Override
  public ExprNodeDesc clone() {
    return new ExprNodeConstantDefaultDesc(typeInfo, value);
  }

  @Override
  public int hashCode() {
    int superHashCode = super.hashCode();
    HashCodeBuilder builder = new HashCodeBuilder();
    builder.appendSuper(superHashCode);
    return builder.toHashCode();
  }
}
