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
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf.StrictChecks;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMacro;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
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
  private List<ExprNodeDesc> chidren;
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
    this.chidren = children;
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
    return writableObjectInspector;
  }

  public GenericUDF getGenericUDF() {
    return genericUDF;
  }

  public void setGenericUDF(GenericUDF genericUDF) {
    this.genericUDF = genericUDF;
  }

  public void setChildren(List<ExprNodeDesc> children) {
    chidren = children;
  }

  @Override
  public List<ExprNodeDesc> getChildren() {
    return chidren;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(genericUDF.getClass().getSimpleName());
    if (genericUDF instanceof GenericUDFBridge) {
      GenericUDFBridge genericUDFBridge = (GenericUDFBridge) genericUDF;
      sb.append(" ==> ");
      sb.append(genericUDFBridge.getUdfName());
      sb.append(" ");
    }
    sb.append("(");
    if (chidren != null) {
      for (int i = 0; i < chidren.size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(chidren.get(i));
      }
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public String getExprString() {
    // Get the children expr strings
    String[] childrenExprStrings = new String[chidren.size()];
    for (int i = 0; i < childrenExprStrings.length; i++) {
      childrenExprStrings[i] = chidren.get(i).getExprString();
    }

    return genericUDF.getDisplayString(childrenExprStrings);
  }

  @Override
  public List<String> getCols() {
    List<String> colList = new ArrayList<String>();
    if (chidren != null) {
      int pos = 0;
      while (pos < chidren.size()) {
        List<String> colCh = chidren.get(pos).getCols();
        colList = Utilities.mergeUniqElems(colList, colCh);
        pos++;
      }
    }

    return colList;
  }

  @Override
  public ExprNodeDesc clone() {
    List<ExprNodeDesc> cloneCh = new ArrayList<ExprNodeDesc>(chidren.size());
    for (ExprNodeDesc ch : chidren) {
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
      String funcText,
      List<ExprNodeDesc> children) throws UDFArgumentException {
    ObjectInspector[] childrenOIs = new ObjectInspector[children.size()];
    for (int i = 0; i < childrenOIs.length; i++) {
      childrenOIs[i] = children.get(i).getWritableObjectInspector();
    }

    // Check if a bigint is implicitely cast to a double as part of a comparison
    // Perform the check here instead of in GenericUDFBaseCompare to guarantee it is only run once per operator
    if (genericUDF instanceof GenericUDFBaseCompare && children.size() == 2) {

      TypeInfo oiTypeInfo0 = children.get(0).getTypeInfo();
      TypeInfo oiTypeInfo1 = children.get(1).getTypeInfo();

      SessionState ss = SessionState.get();
      Configuration conf = (ss != null) ? ss.getConf() : new Configuration();

      LogHelper console = new LogHelper(LOG);

      // For now, if a bigint is going to be cast to a double throw an error or warning
      if ((oiTypeInfo0.equals(TypeInfoFactory.stringTypeInfo) && oiTypeInfo1.equals(TypeInfoFactory.longTypeInfo)) ||
          (oiTypeInfo0.equals(TypeInfoFactory.longTypeInfo) && oiTypeInfo1.equals(TypeInfoFactory.stringTypeInfo))) {
        String error = StrictChecks.checkTypeSafety(conf);
        if (error != null) throw new UDFArgumentException(error);
        console.printError("WARNING: Comparing a bigint and a string may result in a loss of precision.");
      } else if ((oiTypeInfo0.equals(TypeInfoFactory.doubleTypeInfo) && oiTypeInfo1.equals(TypeInfoFactory.longTypeInfo)) ||
          (oiTypeInfo0.equals(TypeInfoFactory.longTypeInfo) && oiTypeInfo1.equals(TypeInfoFactory.doubleTypeInfo))) {
        String error = StrictChecks.checkTypeSafety(conf);
        if (error != null) throw new UDFArgumentException(error);
        console.printError("WARNING: Comparing a bigint and a double may result in a loss of precision.");
      }
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

    if (chidren.size() != dest.getChildren().size()) {
      return false;
    }

    for (int pos = 0; pos < chidren.size(); pos++) {
      if (!chidren.get(pos).isSame(dest.getChildren().get(pos))) {
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
    builder.append(chidren);
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
