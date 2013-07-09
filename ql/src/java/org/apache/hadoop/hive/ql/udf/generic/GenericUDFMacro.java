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

package org.apache.hadoop.hive.ql.udf.generic;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * GenericUDFMacro wraps a user-defined macro expression into a GenericUDF
 * interface.
 */
public class GenericUDFMacro extends GenericUDF implements Serializable {

  private static final long serialVersionUID = 2829755821687181020L;
  private String macroName;
  private ExprNodeDesc bodyDesc;
  private transient ExprNodeEvaluator body;
  private List<String> colNames;
  private List<TypeInfo> colTypes;
  private transient ObjectInspectorConverters.Converter converters[];
  private transient ArrayList<Object> evaluatedArguments;

  public GenericUDFMacro(String macroName, ExprNodeDesc bodyDesc,
                         List<String> colNames, List<TypeInfo> colTypes) {

    this.macroName = macroName;
    this.bodyDesc = bodyDesc;
    this.colNames = colNames;
    this.colTypes = colTypes;
    assert(this.bodyDesc != null);
    assert(colNames.size() == colTypes.size());
  }

  // For serialization only.
  public GenericUDFMacro() {

  }

  public boolean isDeterministic() {
    if(body != null) {
      return body.isDeterministic();
    }
    return true;
  }
  
  public boolean isStateful() {
    if(body != null) {
      return body.isStateful();
    }
    return false;
  }
 
  private void checkNotNull(Object object, String msg) {
    if(object == null) {
      throw new NullPointerException(msg);
    }
  }
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkNotNull(colTypes, "colTypes");
    checkNotNull(arguments, "arguments");
    checkNotNull(bodyDesc, "bodyDesc");
    if(colTypes.size() != arguments.length) {
      throw new UDFArgumentLengthException(
          "The macro " + macroName + " accepts exactly " + colTypes.size() + " arguments.");
    }
    try {
      body = ExprNodeEvaluatorFactory.get(bodyDesc);
    } catch (HiveException ex) {
      throw new UDFArgumentException(ex);
    }
    converters = new ObjectInspectorConverters.Converter[arguments.length];
    ArrayList<ObjectInspector> colObjectInspectors = new ArrayList<ObjectInspector>(colTypes.size());
    for (int index = 0; index < arguments.length; ++index) {
      ObjectInspector objectInspector = TypeInfoUtils.
          getStandardWritableObjectInspectorFromTypeInfo(colTypes.get(index));
      colObjectInspectors.add(objectInspector);
      converters[index] =
          ObjectInspectorConverters.getConverter(arguments[index], objectInspector);
    }
    evaluatedArguments = new ArrayList<Object>(arguments.length);
    ObjectInspector structOI = ObjectInspectorFactory
        .getStandardStructObjectInspector(colNames, colObjectInspectors);
    try {
      return body.initialize(structOI);
    } catch (HiveException ex) {
      throw new UDFArgumentException(ex);
    }
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    evaluatedArguments.clear();
    for (int index = 0; index < arguments.length; ++index) {
      evaluatedArguments.add(converters[index].convert(arguments[index].get()));
    }
    return body.evaluate(evaluatedArguments);
  }

  @Override
  public String getDisplayString(String[] children) {
      StringBuilder sb = new StringBuilder();
      sb.append(macroName);
      sb.append("(");
      for (int i = 0; i < children.length; i++) {
        sb.append(children[i]);
        if (i + 1 < children.length) {
          sb.append(", ");
        }
      }
      sb.append(")");
      return sb.toString();
  }

  public void setMacroName(String macroName) {
    this.macroName = macroName;
  }
  public String getMacroName() {
    return macroName;
  }

  public void setBody(ExprNodeDesc bodyDesc) {
    this.bodyDesc = bodyDesc;
  }
  public ExprNodeDesc getBody() {
    return bodyDesc;
  }

  public void setColNames(List<String> colNames) {
    this.colNames = colNames;
  }
  public List<String> getColNames() {
    return colNames;
  }

  public void setColTypes(List<TypeInfo> colTypes) {
    this.colTypes = colTypes;
  }
  public List<TypeInfo> getColTypes() {
    return colTypes;
  }
}
