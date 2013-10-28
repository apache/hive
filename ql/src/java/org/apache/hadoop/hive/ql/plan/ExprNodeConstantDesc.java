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
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * A constant expression.
 */
public class ExprNodeConstantDesc extends ExprNodeDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  private Object value;

  public ExprNodeConstantDesc() {
  }

  public ExprNodeConstantDesc(TypeInfo typeInfo, Object value) {
    super(typeInfo);
    this.value = value;
  }

  public ExprNodeConstantDesc(Object value) {
    this(TypeInfoFactory
        .getPrimitiveTypeInfoFromJavaPrimitive(value.getClass()), value);
  }

  public void setValue(Object value) {
    this.value = value;
  }

  public Object getValue() {
    return value;
  }

  @Override
  public ConstantObjectInspector getWritableObjectInspector() {
    PrimitiveTypeInfo pti = (PrimitiveTypeInfo) getTypeInfo();
    PrimitiveCategory pc = pti.getPrimitiveCategory();
    // Convert from Java to Writable
    Object writableValue = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(pti).getPrimitiveWritableObject(
          getValue());
    return PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector((PrimitiveTypeInfo) getTypeInfo(), writableValue);
  }


  @Override
  public String toString() {
    return "Const " + typeInfo.toString() + " " + value;
  }

  @Explain(displayName = "expr")
  @Override
  public String getExprString() {
    if (value == null) {
      return "null";
    }

    if (typeInfo.getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {
      return "'" + value.toString() + "'";
    } else {
      return value.toString();
    }
  }

  @Override
  public ExprNodeDesc clone() {
    return new ExprNodeConstantDesc(typeInfo, value);
  }

  @Override
  public boolean isSame(Object o) {
    if (!(o instanceof ExprNodeConstantDesc)) {
      return false;
    }
    ExprNodeConstantDesc dest = (ExprNodeConstantDesc) o;
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

    return true;
  }

  @Override
  public int hashCode() {
    int superHashCode = super.hashCode();
    HashCodeBuilder builder = new HashCodeBuilder();
    builder.appendSuper(superHashCode);
    builder.append(value);
    return builder.toHashCode();
  }
}
