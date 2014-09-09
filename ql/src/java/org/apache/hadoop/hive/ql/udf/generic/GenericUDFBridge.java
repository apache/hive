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
import java.lang.reflect.Method;
import java.util.ArrayList;

import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ConversionHelper;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * GenericUDFBridge encapsulates UDF to provide the same interface as
 * GenericUDF.
 *
 * Note that GenericUDFBridge implements Serializable because the name of the
 * UDF class needs to be serialized with the plan.
 *
 */
public class GenericUDFBridge extends GenericUDF implements Serializable {
  private static final long serialVersionUID = 4994861742809511113L;

  /**
   * The name of the UDF.
   */
  private String udfName;

  /**
   * Whether the UDF is an operator or not. This controls how the display string
   * is generated.
   */
  private boolean isOperator;

  /**
   * The underlying UDF class Name.
   */
  private String udfClassName;

  /**
   * The underlying method of the UDF class.
   */
  private transient Method udfMethod;

  /**
   * Helper to convert the parameters before passing to udfMethod.
   */
  private transient ConversionHelper conversionHelper;
  /**
   * The actual udf object.
   */
  private transient UDF udf;
  /**
   * The non-deferred real arguments for method invocation.
   */
  private transient Object[] realArguments;

  /**
   * Create a new GenericUDFBridge object.
   *
   * @param udfName
   *          The name of the corresponding udf.
   * @param isOperator true for operators
   * @param udfClassName java class name of UDF
   */
  public GenericUDFBridge(String udfName, boolean isOperator,
      String udfClassName) {
    this.udfName = udfName;
    this.isOperator = isOperator;
    this.udfClassName = udfClassName;
  }
 
  // For Java serialization only
  public GenericUDFBridge() {
  }

  public void setUdfName(String udfName) {
    this.udfName = udfName;
  }

  @Override
  public String getUdfName() {
    return udfName;
  }

  public String getUdfClassName() {
    return udfClassName;
  }

  public void setUdfClassName(String udfClassName) {
    this.udfClassName = udfClassName;
  }

  public boolean isOperator() {
    return isOperator;
  }

  public void setOperator(boolean isOperator) {
    this.isOperator = isOperator;
  }

  public Class<? extends UDF> getUdfClass() {
    try {
      return (Class<? extends UDF>) Class.forName(udfClassName, true, Utilities.getSessionSpecifiedClassLoader());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    try {
      udf = (UDF) Class.forName(udfClassName, true, Utilities.getSessionSpecifiedClassLoader()).newInstance();
    } catch (Exception e) {
      throw new UDFArgumentException(
          "Unable to instantiate UDF implementation class " + udfClassName + ": " + e);
    }

    // Resolve for the method based on argument types
    ArrayList<TypeInfo> argumentTypeInfos = new ArrayList<TypeInfo>(
        arguments.length);
    for (ObjectInspector argument : arguments) {
      argumentTypeInfos.add(TypeInfoUtils
          .getTypeInfoFromObjectInspector(argument));
    }
    udfMethod = udf.getResolver().getEvalMethod(argumentTypeInfos);
    udfMethod.setAccessible(true);

    // Create parameter converters
    conversionHelper = new ConversionHelper(udfMethod, arguments);

    // Create the non-deferred realArgument
    realArguments = new Object[arguments.length];

    // Get the return ObjectInspector.
    ObjectInspector returnOI = ObjectInspectorFactory
        .getReflectionObjectInspector(udfMethod.getGenericReturnType(),
        ObjectInspectorOptions.JAVA);

    return returnOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    assert (arguments.length == realArguments.length);

    // Calculate all the arguments
    for (int i = 0; i < realArguments.length; i++) {
      realArguments[i] = arguments[i].get();
    }

    // Call the function
    Object result = FunctionRegistry.invoke(udfMethod, udf, conversionHelper
        .convertIfNecessary(realArguments));

    // For non-generic UDF, type info isn't available. This poses a problem for Hive Decimal.
    // If the returned value is HiveDecimal, we assume maximum precision/scale.
    if (result != null && result instanceof HiveDecimalWritable) {
      result = HiveDecimalUtils.enforcePrecisionScale((HiveDecimalWritable) result,
          HiveDecimal.SYSTEM_DEFAULT_PRECISION, HiveDecimal.SYSTEM_DEFAULT_SCALE);
    }

    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    if (isOperator) {
      if (children.length == 1) {
        // Prefix operator
        return "(" + udfName + " " + children[0] + ")";
      } else {
        // Infix operator
        assert children.length == 2;
        return "(" + children[0] + " " + udfName + " " + children[1] + ")";
      }
    } else {
      StringBuilder sb = new StringBuilder();
      sb.append(udfName);
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
  }

  @Override
  public String[] getRequiredJars() {
    return udf.getRequiredJars();
  }

  @Override
  public String[] getRequiredFiles() {
    return udf.getRequiredFiles();
  }

}
