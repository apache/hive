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
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;

/**
 * The reason that we have to store UDFClass as well as UDFMethod is because
 * UDFMethod might be declared in a parent class of UDFClass. As a result,
 * UDFMethod.getDeclaringClass() may not work.
 */
public class exprNodeFuncDesc extends exprNodeDesc implements Serializable {

  private static final long serialVersionUID = 1L;
  private Class<? extends UDF> UDFClass;
  private Method UDFMethod;
  private List<exprNodeDesc> childExprs; 
  
  public exprNodeFuncDesc() {}
  public exprNodeFuncDesc(TypeInfo typeInfo, Class<? extends UDF> UDFClass, 
                          Method UDFMethod, List<exprNodeDesc> children) {
    super(typeInfo);
    assert(UDFClass != null);
    this.UDFClass = UDFClass;
    assert(UDFMethod != null);
    this.UDFMethod = UDFMethod;
    this.childExprs = children;
  }
  
  public Class<? extends UDF> getUDFClass() {
    return UDFClass;
  }
  
  public void setUDFClass(Class<? extends UDF> UDFClass) {
    this.UDFClass = UDFClass;
  }
  public Method getUDFMethod() {
    return this.UDFMethod;
  }
  public void setUDFMethod(Method method) {
    this.UDFMethod = method;
  }
  public List<exprNodeDesc> getChildExprs() {
    return this.childExprs;
  }
  public void setChildExprs(List<exprNodeDesc> children) {
    this.childExprs = children;
  }
  @Override
  public List<? extends Node> getChildren() {
    return (List<? extends Node>)this.childExprs;
  }
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(UDFClass.toString());
    sb.append(".");
    sb.append(UDFMethod.toString());
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
    FunctionInfo fI = FunctionRegistry.getUDFInfo(UDFClass);
    StringBuilder sb = new StringBuilder();
    
    if (fI.getOpType() == FunctionInfo.OperatorType.PREFIX ||
        fI.isAggFunction()) {
      sb.append(fI.getDisplayName());
      if (!fI.isOperator()) {
        sb.append("(");
      }
      else {
        sb.append(" ");
      }
      
      boolean first = true;
      for(exprNodeDesc chld: childExprs) {
        if (!first) {
          sb.append(", ");
        }
        first = false;
        
        sb.append(chld.getExprString());
      }
      
      if(!fI.isOperator()) {
        sb.append(")");
      }
    }
    else if (fI.getOpType() == FunctionInfo.OperatorType.INFIX) {
      // assert that this has only 2 children
      assert(childExprs.size() == 2);
      sb.append("(");
      sb.append(childExprs.get(0).getExprString());
      sb.append(" ");
      sb.append(fI.getDisplayName());
      sb.append(" ");
      sb.append(childExprs.get(1).getExprString());
      sb.append(")");
    }
    else if (fI.getOpType() == FunctionInfo.OperatorType.POSTFIX) {
      // assert for now as there should be no such case
      assert(childExprs.size() == 1);
      sb.append(childExprs.get(0).getExprString());
      sb.append(" ");
      sb.append(fI.getDisplayName());
    }
    
    return sb.toString();
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
    exprNodeFuncDesc clone = new exprNodeFuncDesc(this.typeInfo,
        this.UDFClass, this.UDFMethod, cloneCh);
    return clone;
  }
  
  @Override
  public boolean isSame(Object o) {
    if (!(o instanceof exprNodeFuncDesc))
      return false;
    exprNodeFuncDesc dest = (exprNodeFuncDesc)o;
    if (!typeInfo.equals(dest.getTypeInfo()) ||
        !UDFClass.equals(dest.getUDFClass()) ||
        !UDFMethod.equals(dest.getUDFMethod()))
      return false;
    
    if (childExprs.size() != dest.getChildExprs().size())
      return false;
    
    for (int pos = 0; pos < childExprs.size(); pos++) {
      if (!childExprs.get(pos).isSame(dest.getChildExprs().get(pos)))
        return false;
    }
    
    return true; 
  }
}
