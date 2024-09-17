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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSortedMultiset;

import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMacro;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * Describes a GenericFunc node.
 */
public class ExprNodeGenericFuncDesc extends ExprNodeDesc implements
    Serializable {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory
      .getLogger(ExprNodeGenericFuncDesc.class.getName());

  /**
   * In case genericUDF is Serializable, we will serialize the object.
   *
   * In case genericUDF does not implement Serializable, Java will remember the
   * class of genericUDF and creates a new instance when deserialized. This is
   * exactly what we want.
   */
  private GenericUDF genericUDF;
  private List<ExprNodeDesc> children;
  private transient String funcText;
  /**
   * This class uses a writableObjectInspector rather than a TypeInfo to store
   * the canonical type information for this NodeDesc.
   */
  private transient ObjectInspector writableObjectInspector;
  //Is this an expression that should perform a comparison for sorted searches
  private boolean isSortedExpr;

  public ExprNodeGenericFuncDesc() {;
  }

  /* If the function has an explicit name like func(args) then call a
   * constructor that explicitly provides the function name in the
   * funcText argument.
   */
  public ExprNodeGenericFuncDesc(TypeInfo typeInfo, GenericUDF genericUDF,
      String funcText,
      List<ExprNodeDesc> children) {
    this(TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(typeInfo),
         genericUDF, funcText, children);
  }

  public ExprNodeGenericFuncDesc(ObjectInspector oi, GenericUDF genericUDF,
      String funcText,
      List<ExprNodeDesc> children) {
    super(TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
    this.writableObjectInspector =
        ObjectInspectorUtils.getWritableObjectInspector(oi);
    assert (genericUDF != null);
    this.genericUDF = genericUDF;
    this.children = children;
    this.funcText = funcText;
  }

  // Backward-compatibility interfaces for functions without a user-visible name.
  public ExprNodeGenericFuncDesc(TypeInfo typeInfo, GenericUDF genericUDF,
      List<ExprNodeDesc> children) {
    this(typeInfo, genericUDF, null, children);
  }

  public ExprNodeGenericFuncDesc(ObjectInspector oi, GenericUDF genericUDF,
      List<ExprNodeDesc> children) {
    this(oi, genericUDF, null, children);
  }

  @Override
  public ObjectInspector getWritableObjectInspector() {
    if (writableObjectInspector == null) {
      writableObjectInspector = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(typeInfo);
    }
    return writableObjectInspector;
  }

  public GenericUDF getGenericUDF() {
    return genericUDF;
  }

  public void setGenericUDF(GenericUDF genericUDF) {
    this.genericUDF = genericUDF;
  }

  public void setChildren(List<ExprNodeDesc> children) {
    this.children = children;
  }

  @Override
  public List<ExprNodeDesc> getChildren() {
    return children;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(genericUDF.getClass().getSimpleName());
    if (genericUDF instanceof GenericUDFBridge) {
      GenericUDFBridge genericUDFBridge = (GenericUDFBridge) genericUDF;
      sb.append(" ==> ");
      String udfName = genericUDFBridge.getUdfName();
      Class<? extends UDF> udfClass = genericUDFBridge.getUdfClass();
      sb.append(udfName != null ? udfName : (udfClass != null ? udfClass.getSimpleName() : "null"));
      sb.append(" ");
    }
    sb.append("(");
    if (children != null) {
      for (int i = 0; i < children.size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(children.get(i));
      }
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public String getExprString() {
    // Get the children expr strings
    String[] childrenExprStrings = new String[children.size()];
    for (int i = 0; i < childrenExprStrings.length; i++) {
      childrenExprStrings[i] = children.get(i).getExprString();
    }

    return genericUDF.getDisplayString(childrenExprStrings);
  }

  @Override
  public String getExprString(boolean sortChildren) {
    if (sortChildren) {
      UDFType udfType = genericUDF.getClass().getAnnotation(UDFType.class);
      if (udfType.commutative()) {
        // Get the sorted children expr strings
        String[] childrenExprStrings = new String[children.size()];
        for (int i = 0; i < childrenExprStrings.length; i++) {
          childrenExprStrings[i] = children.get(i).getExprString();
        }
        return genericUDF.getDisplayString(
            ImmutableSortedMultiset.copyOf(childrenExprStrings).toArray(new String[childrenExprStrings.length]));
      }
    }
    return getExprString();
  }

  @Override
  public List<String> getCols() {
    List<String> colList = new ArrayList<String>();
    if (children != null) {
      int pos = 0;
      while (pos < children.size()) {
        List<String> colCh = children.get(pos).getCols();
        colList = Utilities.mergeUniqElems(colList, colCh);
        pos++;
      }
    }

    return colList;
  }

  @Override
  public ExprNodeDesc clone() {
    List<ExprNodeDesc> cloneCh = new ArrayList<ExprNodeDesc>(children.size());
    for (ExprNodeDesc ch : children) {
      cloneCh.add(ch.clone());
    }
    ExprNodeGenericFuncDesc clone = new ExprNodeGenericFuncDesc(typeInfo,
        FunctionRegistry.cloneGenericUDF(genericUDF), funcText, cloneCh);
    return clone;
  }

  /**
   * Create a ExprNodeGenericFuncDesc based on the genericUDFClass and the
   * children parameters. If the function has an explicit name, the
   * newInstance method should be passed the function name in the funcText
   * argument.
   *
   * @throws UDFArgumentException
   */
  public static ExprNodeGenericFuncDesc newInstance(GenericUDF genericUDF,
      String funcText, List<ExprNodeDesc> children) throws UDFArgumentException {
    ObjectInspector[] childrenOIs = new ObjectInspector[children.size()];
    for (int i = 0; i < childrenOIs.length; i++) {
      childrenOIs[i] = children.get(i).getWritableObjectInspector();
    }

    ObjectInspector oi = genericUDF.initializeAndFoldConstants(childrenOIs);

    String[] requiredJars = genericUDF.getRequiredJars();
    String[] requiredFiles = genericUDF.getRequiredFiles();
    SessionState ss = SessionState.get();

    if (requiredJars != null) {
      SessionState.ResourceType t = SessionState.find_resource_type("JAR");
      try {
        ss.add_resources(t, Arrays.asList(requiredJars));
      } catch (Exception e) {
        throw new UDFArgumentException(e);
      }
    }

    if (requiredFiles != null) {
      SessionState.ResourceType t = SessionState.find_resource_type("FILE");
      try {
        ss.add_resources(t, Arrays.asList(requiredFiles));
      } catch (Exception e) {
        throw new UDFArgumentException(e);
      }
    }

    return new ExprNodeGenericFuncDesc(oi, genericUDF, funcText, children);
  }

  /* Backward-compatibility interface for the case where there is no explicit
   * name for the function.
   */
  public static ExprNodeGenericFuncDesc newInstance(GenericUDF genericUDF,
    List<ExprNodeDesc> children) throws UDFArgumentException {
    return newInstance(genericUDF, null, children);
  }

  @Override
  public boolean isSame(Object o) {
    if (!(o instanceof ExprNodeGenericFuncDesc)) {
      return false;
    }
    ExprNodeGenericFuncDesc dest = (ExprNodeGenericFuncDesc) o;
    if (!typeInfo.equals(dest.getTypeInfo())
        || !genericUDF.getClass().equals(dest.getGenericUDF().getClass())) {
      return false;
    }

    if (genericUDF instanceof GenericUDFBridge) {
      GenericUDFBridge bridge = (GenericUDFBridge) genericUDF;
      GenericUDFBridge bridge2 = (GenericUDFBridge) dest.getGenericUDF();
      if (!bridge.getUdfClassName().equals(bridge2.getUdfClassName())
          || !bridge.getUdfName().equals(bridge2.getUdfName())
          || bridge.isOperator() != bridge2.isOperator()) {
        return false;
      }
    }

    if (genericUDF instanceof GenericUDFMacro) {
      // if getMacroName is null, we always treat it different from others.
      if (((GenericUDFMacro) genericUDF).getMacroName() == null
          || !(((GenericUDFMacro) genericUDF).getMacroName()
              .equals(((GenericUDFMacro) dest.genericUDF).getMacroName()))) {
        return false;
      }
    }

    if (children.size() != dest.getChildren().size()) {
      return false;
    }

    for (int pos = 0; pos < children.size(); pos++) {
      if (!children.get(pos).isSame(dest.getChildren().get(pos))) {
        return false;
      }
    }

    return true;
  }

  @Override
  public int hashCode() {
    int superHashCode = super.hashCode();
    HashCodeBuilder builder = new HashCodeBuilder();
    builder.appendSuper(superHashCode);
    builder.append(children);
    return builder.toHashCode();
  }

  public boolean isSortedExpr() {
    return isSortedExpr;
  }

  public void setSortedExpr(boolean isSortedExpr) {
    this.isSortedExpr = isSortedExpr;
  }

  public String getFuncText() {
    return this.funcText;
  }
}
