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

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.ql.exec.Utilities;


public class exprNodeFieldDesc extends exprNodeDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  exprNodeDesc desc;
  String fieldName;
  
  // Used to support a.b where a is a list of struct that contains a field called b.
  // a.b will return an array that contains field b of all elements of array a. 
  Boolean isList;
  
  public exprNodeFieldDesc() {}
  public exprNodeFieldDesc(TypeInfo typeInfo, exprNodeDesc desc, String fieldName, Boolean isList) {
    super(typeInfo);
    this.desc = desc;
    this.fieldName = fieldName;
    this.isList = isList;
  }
  
  @Override
  public List<exprNodeDesc> getChildren() {
    List<exprNodeDesc> children = new ArrayList<exprNodeDesc>(2);
    children.add(desc);
    return children;
  }
  
  public exprNodeDesc getDesc() {
    return this.desc;
  }
  public void setDesc(exprNodeDesc desc) {
    this.desc = desc;
  }
  public String getFieldName() {
    return this.fieldName;
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
    return this.desc.toString() + "." + this.fieldName;
  }
  
  @explain(displayName="expr")
  @Override
  public String getExprString() {
    return this.desc.getExprString() + "." + this.fieldName;
  }

  public List<String> getCols() {
    List<String> colList = new ArrayList<String>();
    if (desc != null) 
    	colList = Utilities.mergeUniqElems(colList, desc.getCols());    
    return colList;
  }
  @Override
  public exprNodeDesc clone() {
    return new exprNodeFieldDesc(this.typeInfo, this.desc, this.fieldName, this.isList);
  }

  @Override
  public boolean isSame(Object o) {
    if (!(o instanceof exprNodeFieldDesc))
      return false;
    exprNodeFieldDesc dest = (exprNodeFieldDesc)o;
    if (!typeInfo.equals(dest.getTypeInfo()))
      return false;
    if (!fieldName.equals(dest.getFieldName()) ||
        !isList.equals(dest.getIsList()) ||
        !desc.isSame(dest.getDesc()))
      return false;
      
    return true; 
  }
}
