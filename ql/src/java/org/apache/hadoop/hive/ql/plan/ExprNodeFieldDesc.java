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
import java.util.List;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * ExprNodeFieldDesc.
 *
 */
public class ExprNodeFieldDesc extends ExprNodeDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  ExprNodeDesc desc;
  String fieldName;

  // Used to support a.b where a is a list of struct that contains a field
  // called b.
  // a.b will return an array that contains field b of all elements of array a.
  Boolean isList;

  public ExprNodeFieldDesc() {
  }

  public ExprNodeFieldDesc(TypeInfo typeInfo, ExprNodeDesc desc,
      String fieldName, Boolean isList) {
    super(typeInfo);
    this.desc = desc;
    this.fieldName = fieldName;
    this.isList = isList;
  }

  @Override
  public List<ExprNodeDesc> getChildren() {
    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>(2);
    children.add(desc);
    return children;
  }

  public ExprNodeDesc getDesc() {
    return desc;
  }

  public void setDesc(ExprNodeDesc desc) {
    this.desc = desc;
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public Boolean getIsList() {
    return isList;
  }

  public void setIsList(Boolean isList) {
    this.isList = isList;
  }

  @Override
  public String toString() {
    return desc.toString() + "." + fieldName;
  }

  @Explain(displayName = "expr")
  @Override
  public String getExprString() {
    return desc.getExprString() + "." + fieldName;
  }

  @Override
  public List<String> getCols() {
    List<String> colList = new ArrayList<String>();
    if (desc != null) {
      colList = Utilities.mergeUniqElems(colList, desc.getCols());
    }
    return colList;
  }

  @Override
  public ExprNodeDesc clone() {
    return new ExprNodeFieldDesc(typeInfo, desc, fieldName, isList);
  }

  @Override
  public boolean isSame(Object o) {
    if (!(o instanceof ExprNodeFieldDesc)) {
      return false;
    }
    ExprNodeFieldDesc dest = (ExprNodeFieldDesc) o;
    if (!typeInfo.equals(dest.getTypeInfo())) {
      return false;
    }
    if (!fieldName.equals(dest.getFieldName())
        || !isList.equals(dest.getIsList()) || !desc.isSame(dest.getDesc())) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int superHashCode = super.hashCode();
    HashCodeBuilder builder = new HashCodeBuilder();
    builder.appendSuper(superHashCode);
    builder.append(desc);
    builder.append(fieldName);
    builder.append(isList);
    return builder.toHashCode();
  }
}
