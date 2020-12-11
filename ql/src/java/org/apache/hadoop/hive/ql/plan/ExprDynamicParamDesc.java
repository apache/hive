/*
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
import java.util.List;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hive.common.StringInternUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * An expression representing dynamic parameter.
 * This is required for Prepare/Execute statements
 */
public class ExprDynamicParamDesc extends ExprNodeDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  final protected transient static char[] hexArray = "0123456789ABCDEF".toCharArray();

  private int index;
  private Object value;

  public ExprDynamicParamDesc() {
  }

  public ExprDynamicParamDesc(TypeInfo typeInfo, int index, Object value) {
    super(typeInfo);
    this.index =  index;
    this.value = value;
  }

  public Object getValue() {
    return value;
  }

  public int getIndex() {
    return index;
  }


  @Override
  public String toString() {
    return "$" + index;
  }

  @Override
  public String getExprString() {
    return toString();
  }

  @Override
  public ExprNodeDesc clone() {
    return new ExprDynamicParamDesc(typeInfo, index, value);
  }

  @Override
  public boolean isSame(Object o) {
    if (!(o instanceof ExprDynamicParamDesc)) {
      return false;
    }
    ExprDynamicParamDesc dest = (ExprDynamicParamDesc) o;
    if (!typeInfo.equals(dest.getTypeInfo())) {
      return false;
    }
    if (value == null) {
      if (dest.getValue() != null) {
        return false;
      }
    } else if (!value.equals(dest.getValue())) {
      return false;
    }
    if (dest.getIndex() != index) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int superHashCode = super.hashCode();
    HashCodeBuilder builder = new HashCodeBuilder();
    builder.appendSuper(superHashCode);
    builder.append(index);
    if (value != null) {
      builder.append(value);
    }
    return builder.toHashCode();
  }
}
