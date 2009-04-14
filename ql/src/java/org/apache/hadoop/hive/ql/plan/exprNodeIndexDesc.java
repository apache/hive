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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;


/**
 * exprNodeIndexDesc describes the operation of getting a list element out
 * of a list, and getting a value out of a map.  
 */
public class exprNodeIndexDesc extends exprNodeDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  exprNodeDesc desc;
  exprNodeDesc index;
  
  public exprNodeIndexDesc() {}
  public exprNodeIndexDesc(TypeInfo typeInfo, exprNodeDesc desc, exprNodeDesc index) {
    super(typeInfo);
    this.desc = desc;
    this.index = index;    
  }
  public exprNodeIndexDesc(exprNodeDesc desc, exprNodeDesc index) {
    super( desc.getTypeInfo().getCategory().equals(Category.LIST)
        ? ((ListTypeInfo)desc.getTypeInfo()).getListElementTypeInfo()
        : ((MapTypeInfo)desc.getTypeInfo()).getMapValueTypeInfo());
    this.desc = desc;
    this.index = index;    
  }
  
  @Override
  public List<? extends Node> getChildren() {
    List<Node> children = new ArrayList<Node>(2);
    children.add(desc);
    children.add(index);
    return children;
  }
  
  public exprNodeDesc getDesc() {
    return this.desc;
  }
  public void setDesc(exprNodeDesc desc) {
    this.desc = desc;
  }
  public exprNodeDesc getIndex() {
    return this.index;
  }
  public void setIndex(exprNodeDesc index) {
    this.index = index;
  }
  @Override
  public String toString() {
    return this.desc.toString() + "[" + this.index + "]";
  }
  
  @explain(displayName="expr")
  @Override
  public String getExprString() {
    return this.desc.getExprString() + "[" + this.index.getExprString() + "]";
  }
  
  public List<String> getCols() {
    List<String> colList = new ArrayList<String>();
    if (desc != null) 
    	colList = Utilities.mergeUniqElems(colList, desc.getCols());
    if (index != null)
    	colList = Utilities.mergeUniqElems(colList, index.getCols());
    
    return colList;
  }
  @Override
  public exprNodeDesc clone() {
    return new exprNodeIndexDesc(this.typeInfo, this.desc, this.index);
  }
}
