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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringUpper;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.StringConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * UDFUpper.
 *
 */
@Description(name = "upper,ucase",
    value = "_FUNC_(str) - Returns str with all characters changed to uppercase",
    extended = "Example:\n"
    + "  > SELECT _FUNC_('Facebook') FROM src LIMIT 1;\n" + "  'FACEBOOK'")
@VectorizedExpressions({StringUpper.class})
public class GenericUDFUpper extends GenericUDF {
  private transient PrimitiveObjectInspector argumentOI;
  private transient StringConverter stringConverter;
  private transient PrimitiveCategory returnType = PrimitiveCategory.STRING;
  private transient GenericUDFUtils.StringHelper returnHelper;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentLengthException(
          "UPPER requires 1 argument, got " + arguments.length);
    }

    if (arguments[0].getCategory() != Category.PRIMITIVE) {
      throw new UDFArgumentException(
          "UPPER only takes primitive types, got " + arguments[0].getTypeName());
    }
    argumentOI = (PrimitiveObjectInspector) arguments[0];

    stringConverter = new PrimitiveObjectInspectorConverter.StringConverter(argumentOI);
    PrimitiveCategory inputType = argumentOI.getPrimitiveCategory();
    ObjectInspector outputOI = null;
    BaseCharTypeInfo typeInfo;
    switch (inputType) {
      case CHAR:
        // return type should have same length as the input.
        returnType = inputType;
        typeInfo = TypeInfoFactory.getCharTypeInfo(
            GenericUDFUtils.StringHelper.getFixedStringSizeForType(argumentOI));
        outputOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
            typeInfo);
        break;
      case VARCHAR:
        // return type should have same length as the input.
        returnType = inputType;
        typeInfo = TypeInfoFactory.getVarcharTypeInfo(
            GenericUDFUtils.StringHelper.getFixedStringSizeForType(argumentOI));
        outputOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
            typeInfo);
        break;
      default:
        returnType = PrimitiveCategory.STRING;
        outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        break;
    }
    returnHelper = new GenericUDFUtils.StringHelper(returnType);
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    String val = null;
    if (arguments[0] != null) {
      val = (String) stringConverter.convert(arguments[0].get());
    }
    if (val == null) {
      return null;
    }
    val = val.toUpperCase();
    return returnHelper.setReturnValue(val);
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("upper", children);
  }

}
