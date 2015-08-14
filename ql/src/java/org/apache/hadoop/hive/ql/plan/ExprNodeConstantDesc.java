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
import java.util.List;

import org.apache.commons.lang.builder.HashCodeBuilder;
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
 * A constant expression.
 */
public class ExprNodeConstantDesc extends ExprNodeDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  final protected transient static char[] hexArray = "0123456789ABCDEF".toCharArray();
  private Object value;
  // If this constant was created while doing constant folding, foldedFromCol holds the name of
  // original column from which it was folded.
  private transient String foldedFromCol;
  // string representation of folding constant.
  private transient String foldedFromVal;

  public ExprNodeConstantDesc setFoldedFromVal(String foldedFromVal) {
    this.foldedFromVal = foldedFromVal;
    return this;
  }

  public String getFoldedFromVal() {
    return foldedFromVal;
  }

  public String getFoldedFromCol() {
    return foldedFromCol;
  }

  public void setFoldedFromCol(String foldedFromCol) {
    this.foldedFromCol = foldedFromCol;
  }

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
    // Kryo setter
    this.value = value;
  }

  public Object getValue() {
    return value;
  }

  @Override
  public ConstantObjectInspector getWritableObjectInspector() {
    return ObjectInspectorUtils.getConstantObjectInspector(
      TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo), value);
  }


  @Override
  public String toString() {
    return "Const " + typeInfo.toString() + " " + value;
  }

  private static String getFormatted(TypeInfo typeInfo, Object value) {
    if (value == null) {
      return "null";
    }

    if (typeInfo.getTypeName().equals(serdeConstants.STRING_TYPE_NAME) || typeInfo instanceof BaseCharTypeInfo) {
      return "'" + value.toString() + "'";
    } else if (typeInfo.getTypeName().equals(serdeConstants.BINARY_TYPE_NAME)) {
      byte[] bytes = (byte[]) value;
      char[] hexChars = new char[bytes.length * 2];
      for (int j = 0; j < bytes.length; j++) {
        int v = bytes[j] & 0xFF;
        hexChars[j * 2] = hexArray[v >>> 4];
        hexChars[j * 2 + 1] = hexArray[v & 0x0F];
      }
      return new String(hexChars);
    }
    return value.toString();
  }

  @Override
  public String getExprString() {
    if (typeInfo.getCategory() == Category.PRIMITIVE) {
      return getFormatted(typeInfo, value);
    } else if (typeInfo.getCategory() == Category.STRUCT) {
      StringBuilder sb = new StringBuilder();
      sb.append("const struct(");
      List<?> items = (List<?>) getWritableObjectInspector().getWritableConstantValue();
      List<TypeInfo> structTypes = ((StructTypeInfo) typeInfo).getAllStructFieldTypeInfos();
      for (int i = 0; i < structTypes.size(); i++) {
        final Object o = (i < items.size()) ? items.get(i) : null;
        sb.append(getFormatted(structTypes.get(i), o)).append(",");
      }
      sb.setCharAt(sb.length() - 1, ')');
      return sb.toString();
    } else {
      // unknown type
      return toString();
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
