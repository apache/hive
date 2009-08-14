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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;

/**
 * Describes a GenericFunc node.
 */
public class exprNodeGenericFuncDesc extends exprNodeDesc implements Serializable {

  private static final long serialVersionUID = 1L;
  
  /**
   * In case genericUDF is Serializable, we will serialize the object.
   * 
   * In case genericUDF does not implement Serializable, Java will remember the
   * class of genericUDF and creates a new instance when deserialized.  This is
   * exactly what we want.
   */
  private GenericUDF genericUDF;
  private List<exprNodeDesc> childExprs; 
  
  public exprNodeGenericFuncDesc() {}
  public exprNodeGenericFuncDesc(TypeInfo typeInfo, GenericUDF genericUDF, 
                          List<exprNodeDesc> children) {
    super(typeInfo);
    assert(genericUDF != null);
    this.genericUDF = genericUDF;
    this.childExprs = children;
  }
  
  public GenericUDF getGenericUDF() {
    return genericUDF;
  }
  
  public void setGenericUDF(GenericUDF genericUDF) {
    this.genericUDF = genericUDF;
  }
  
  public List<exprNodeDesc> getChildExprs() {
    return this.childExprs;
  }
  public void setChildExprs(List<exprNodeDesc> children) {
    this.childExprs = children;
  }
  @Override
  public List<exprNodeDesc> getChildren() {
    return childExprs;
  }
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(genericUDF.getClass().toString());
    sb.append("(");
    for(int i=0; i<childExprs.size(); i++) {
      if (i>0) sb.append(", ");
      sb.append(childExprs.get(i).toString());
    }
    sb.append("(");
    sb.append(")");
    return sb.toString();
  }
  
  @explain(displayName="expr")
  @Override
  public String getExprString() {
    // Get the children expr strings
    String[] childrenExprStrings = new String[childExprs.size()];
    for (int i=0; i<childrenExprStrings.length; i++) {
      childrenExprStrings[i] = childExprs.get(i).getExprString();
    }
    
    return genericUDF.getDisplayString(childrenExprStrings);
  }

  public List<String> getCols() {
    List<String> colList = new ArrayList<String>();
    if (childExprs != null) {
      int pos = 0;
      while (pos < childExprs.size()) {
        List<String> colCh = childExprs.get(pos).getCols();
        colList = Utilities.mergeUniqElems(colList, colCh);
        pos++;
      }
    }

    return colList;
  }
  
  @Override
  public exprNodeDesc clone() {
    List<exprNodeDesc> cloneCh = new ArrayList<exprNodeDesc>(childExprs.size());
    for(exprNodeDesc ch :  childExprs) {
      cloneCh.add(ch.clone());
    }
    exprNodeGenericFuncDesc clone = new exprNodeGenericFuncDesc(this.typeInfo,
        FunctionRegistry.cloneGenericUDF(genericUDF), cloneCh);
    return clone;
  }
  
  /**
   * Create a exprNodeGenericFuncDesc based on the genericUDFClass and the children
   * parameters.
   * @throws UDFArgumentException
   */
  public static exprNodeGenericFuncDesc newInstance(GenericUDF genericUDF, 
      List<exprNodeDesc> children) throws UDFArgumentException {
    ObjectInspector[] childrenOIs = new ObjectInspector[children.size()];
    for(int i=0; i<childrenOIs.length; i++) {
      childrenOIs[i] = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
          children.get(i).getTypeInfo());
    }
    
    ObjectInspector oi = genericUDF.initialize(childrenOIs);
    return new exprNodeGenericFuncDesc(TypeInfoUtils.getTypeInfoFromObjectInspector(oi),
        genericUDF, children);
  }
  
  @Override
  public boolean isSame(Object o) {
    if (!(o instanceof exprNodeGenericFuncDesc))
      return false;
    exprNodeGenericFuncDesc dest = (exprNodeGenericFuncDesc)o;
    if (!typeInfo.equals(dest.getTypeInfo()) ||
        !genericUDF.getClass().equals(dest.getGenericUDF().getClass()))
      return false;
    
    if (genericUDF instanceof GenericUDFBridge) {
      GenericUDFBridge bridge = (GenericUDFBridge) genericUDF;
      GenericUDFBridge bridge2 = (GenericUDFBridge) dest.getGenericUDF();
      if (!bridge.getUdfClass().equals(bridge2.getUdfClass())
          || !bridge.getUdfName().equals(bridge2.getUdfName())
          || bridge.isOperator() != bridge2.isOperator())
        return false;
    }
    
    if (childExprs.size() != dest.getChildExprs().size())
      return false;
    
    for (int pos = 0; pos < childExprs.size(); pos++) {
      if (!childExprs.get(pos).isSame(dest.getChildExprs().get(pos)))
        return false;
    }
    
    return true; 
  }
  
}
